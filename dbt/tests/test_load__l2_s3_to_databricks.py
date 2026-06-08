import pytest

from dbt.project.models.load.load__l2_s3_to_databricks import _extract_table_name


@pytest.mark.parametrize(
    "source_file_name,state_id,expected",
    [
        ("VM2--AL--2025-05-10-VOTEHISTORY.tab", "AL", "l2_s3_al_vote_history"),
        ("VM2--NY--2025-05-10-DEMOGRAPHIC.tab", "NY", "l2_s3_ny_demographic"),
        (
            "VM2--CA--2025-05-10-VOTEHISTORY_DataDictionary.csv",
            "CA",
            "l2_s3_ca_vote_history_data_dictionary",
        ),
        (
            "VM2--TX--2025-05-10-DEMOGRAPHIC_DataDictionary.csv",
            "TX",
            "l2_s3_tx_demographic_data_dictionary",
        ),
        # Uniform files
        ("VM2Uniform--AK--2025-05-10.tab", "AK", "l2_s3_ak_uniform"),
        (
            "VM2Uniform--AK--2025-05-10_DataDictionary.csv",
            "AK",
            "l2_s3_ak_uniform_data_dictionary",
        ),
        ("VM2Uniform--AL--2025-05-10.tab", "AL", "l2_s3_al_uniform"),
        (
            "VM2Uniform--AL--2025-05-10_DataDictionary.csv",
            "AL",
            "l2_s3_al_uniform_data_dictionary",
        ),
        ("VM2Uniform--CA--2025-05-10.tab", "CA", "l2_s3_ca_uniform"),
        (
            "VM2Uniform--CA--2025-05-10_DataDictionary.csv",
            "CA",
            "l2_s3_ca_uniform_data_dictionary",
        ),
        ("VM2Uniform--TX--2025-05-10.tab", "TX", "l2_s3_tx_uniform"),
        (
            "VM2Uniform--TX--2025-05-10_DataDictionary.csv",
            "TX",
            "l2_s3_tx_uniform_data_dictionary",
        ),
        ("VM2Uniform--WA--2025-05-10.tab", "WA", "l2_s3_wa_uniform"),
        # Edge cases
        ("VM2--FL--2025-05-10-OTHER.tab", "FL", "l2_s3_fl_other"),
        (
            "VM2--WA--2025-05-10-OTHER_DataDictionary.csv",
            "WA",
            "l2_s3_wa_other_data_dictionary",
        ),
        ("VM2--AL--2025-05-10-VOTEHISTORY.csv", "AL", "l2_s3_al_vote_history"),
        (
            "VM2--AL--2025-05-10-VOTEHISTORY_DataDictionary.tab",
            "AL",
            "l2_s3_al_vote_history_data_dictionary",
        ),
        ("VM2--AL--2025-05-10-VOTEHISTORY.txt", "AL", "l2_s3_al_vote_history"),
    ],
)
def test_extract_table_name(source_file_name, state_id, expected):
    assert _extract_table_name(source_file_name, state_id) == expected
