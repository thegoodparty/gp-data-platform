FROM astrocrpublic.azurecr.io/runtime:3.2-5-python-3.14
# temp: force a dev image rebuild so include/ ships partition_column; remove before merge.

# Ship the dbt project into the image for the load_people_api DAG's Cosmos dbt-test gate
# (DATA-1913). dbt/project lives at the repo root — outside this Astro build context — so fetch
# it with a shallow clone (git is installed via packages.txt). The astro user owns
# /usr/local/airflow, so the project (and its runtime `dbt deps`) is writable.
# Pin to a tag/SHA before merge; tracks the feature branch during PR dev.
RUN git clone --depth 1 --branch feat/DATA-1913-loader-dag \
        https://github.com/thegoodparty/gp-data-platform.git /tmp/gp-dp \
    && mkdir -p /usr/local/airflow/dbt \
    && cp -r /tmp/gp-dp/dbt/project /usr/local/airflow/dbt/project \
    && rm -rf /tmp/gp-dp
