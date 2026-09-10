# scripts/person_clustering.py
"""Constrained clustering for the person entity.

Splink's own clusterer is plain connected components. Closure is sound over
evidence that is transitive (a shared native identifier) and not over evidence
that merely resembles (a Splink score): run over similarity edges it fused two
BallotReady people 6,219 times from an edge set holding no BR-to-BR pair.

So this step closes only over the deterministic links dbt derives from native
identifiers and candidacy clusters, then treats Splink pairs as proposals
between those components. A proposal is admitted only when every pair inside it
was scored a match (a clique) and the result holds at most one BallotReady
person. Anything short of that stays split: a false negative costs a duplicate
profile, a false positive merges two people's HubSpot contacts.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable

import pandas as pd

BR_PREFIX = "ballotready|"
_TS_STAGE_SUFFIX = re.compile(r"__(primary|general|runoff)$")

OUTPUT_COLUMNS = ["record_key", "source_name", "identity_key", "person_group_key", "rejected_reason"]


class UnionFind:
    """Union-find whose root is always the lexically smallest member, so a
    component's label is its min record key and matches dbt's propagation."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        parent = self._parent
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            x, parent[x] = parent[x], root
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[max(ra, rb)] = min(ra, rb)


def is_br(record_key: str) -> bool:
    return record_key.startswith(BR_PREFIX)


def prematch_key(record_key: str, source_name: str) -> str:
    """The prematch keys TechSpeed on the stage-stripped candidate code, so one
    Splink record fans out to every stage record key it covers. Every other
    source is 1:1."""
    return _TS_STAGE_SUFFIX.sub("", record_key) if source_name == "techspeed" else record_key


def _truthy(series: pd.Series) -> pd.Series:
    # er_source and Databricks reads arrive as strings; local CSVs as bools.
    return series.astype(str).str.lower().isin(("true", "1"))


def build_identities(links: pd.DataFrame, nodes: pd.DataFrame) -> dict[str, str]:
    """Connected components over the non-conflicting links.

    A component reaching two BallotReady people along individually sound links
    means one source record is wrong; keeping the smaller BR id would assert one
    of two contradictory identities at even odds, so the component dissolves and
    every member becomes its own identity.
    """
    uf = UnionFind()
    ok = links[~_truthy(links["is_conflict"])]
    for a, b in zip(ok["record_key_1"], ok["record_key_2"], strict=True):
        uf.union(a, b)

    component = {k: uf.find(k) for k in nodes["record_key"]}
    br_per_component: dict[str, int] = defaultdict(int)
    for key, comp in component.items():
        if is_br(key):
            br_per_component[comp] += 1
    contested = {c for c, n in br_per_component.items() if n > 1}
    return {k: (k if c in contested else c) for k, c in component.items()}


def lift_similarities(
    pairwise: pd.DataFrame,
    nodes: pd.DataFrame,
    identity: dict[str, str],
    *,
    threshold: float,
) -> tuple[set[tuple[str, str]], set[str]]:
    """Splink pairs at or above the threshold, expanded from prematch keys to
    record keys and lifted to distinct identity pairs. Sameness inside an
    identity is already asserted, so one member vouching for it is enough.

    Also returns the scored keys absent from the record universe. A pairwise
    vintage is usually older than the nodes it is re-clustered against, so a
    record deleted since scoring drops its pairs here; the caller reports the
    count so drift is visible rather than silent.
    """
    records_for_key: dict[str, list[str]] = defaultdict(list)
    for key, source in zip(nodes["record_key"], nodes["source_name"], strict=True):
        records_for_key[prematch_key(key, source)].append(key)

    scored = pairwise[pd.to_numeric(pairwise["match_probability"]) >= threshold]
    lifted: set[tuple[str, str]] = set()
    unknown_keys: set[str] = set()
    for left, right in zip(scored["unique_id_l"], scored["unique_id_r"], strict=True):
        left_records = records_for_key.get(left)
        right_records = records_for_key.get(right)
        if left_records is None:
            unknown_keys.add(left)
        if right_records is None:
            unknown_keys.add(right)
        if left_records is None or right_records is None:
            continue
        for ka in left_records:
            for kb in right_records:
                ia, ib = identity[ka], identity[kb]
                if ia != ib:
                    lifted.add((min(ia, ib), max(ia, ib)))
    return lifted, unknown_keys


def admit_groups(
    similarities: Iterable[tuple[str, str]],
    identity: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Merge a set of identities only when it is a clique in the similarity
    graph and holds at most one BallotReady person.

    Returns (group_of_identity, rejected_reason_of_identity); identities absent
    from both stand alone.
    """
    br_per_identity: dict[str, int] = defaultdict(int)
    for key, ident in identity.items():
        if is_br(key):
            br_per_identity[ident] += 1

    uf = UnionFind()
    edges: dict[str, int] = defaultdict(int)
    members: dict[str, set[str]] = defaultdict(set)
    similarities = list(similarities)
    for a, b in similarities:
        uf.union(a, b)
    for a, b in similarities:
        comp = uf.find(a)
        edges[comp] += 1
        members[comp].update((a, b))

    group: dict[str, str] = {}
    rejected: dict[str, str] = {}
    for comp, ids in members.items():
        n = len(ids)
        br = sum(br_per_identity[i] for i in ids)
        if br > 1:
            reason = "br_cannot_link"
        elif edges[comp] != n * (n - 1) // 2:
            reason = "incomplete_support"
        else:
            label = min(ids)
            for i in ids:
                group[i] = label
            continue
        for i in ids:
            rejected[i] = reason
    return group, rejected


def cluster_people(
    pairwise: pd.DataFrame,
    links: pd.DataFrame,
    nodes: pd.DataFrame,
    *,
    threshold: float,
) -> pd.DataFrame:
    """One row per node with its identity, its person group, and why a proposed
    merge was refused, if one was. Labels are min record keys, so a group that
    only gains members keeps its label."""
    identity = build_identities(links, nodes)
    similarities, unknown_keys = lift_similarities(pairwise, nodes, identity, threshold=threshold)
    if unknown_keys:
        print(
            f"Scored keys outside the record universe (pairwise/nodes drift): {len(unknown_keys):,}, "
            f"their pairs dropped. First few: {sorted(unknown_keys)[:5]}"
        )
    group, rejected = admit_groups(similarities, identity)

    out = pd.DataFrame(
        {
            "record_key": nodes["record_key"].to_numpy(),
            "source_name": nodes["source_name"].to_numpy(),
        }
    )
    out["identity_key"] = out["record_key"].map(identity)
    out["person_group_key"] = out["identity_key"].map(lambda i: group.get(i, i))
    out["rejected_reason"] = out["identity_key"].map(rejected)
    return out[OUTPUT_COLUMNS]


def summarize(groups: pd.DataFrame) -> str:
    people = groups["person_group_key"].nunique()
    identities = groups["identity_key"].nunique()
    merged = groups.groupby("person_group_key")["identity_key"].nunique()
    refused = groups.dropna(subset=["rejected_reason"]).groupby("rejected_reason")["identity_key"].nunique()
    lines = [
        f"People: {people:,}  |  Identities: {identities:,}  |  Groups fusing 2+ identities: {int((merged > 1).sum()):,}",
        "Refused identities: " + (", ".join(f"{k}={v:,}" for k, v in refused.items()) or "none"),
    ]
    return "\n".join(lines)
