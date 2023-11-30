import sys

import pandas as pd
import requests

url = "https://redcap.kpmp.org/api/"


def dict_extract(existing_df):
    init_columns = [
        "field_name",
        "form_name",
        "section_header",
        "field_type",
        "field_label",
        "select_choices_or_calculations",
        "field_note",
        "text_validation_type_or_show_slider_number",
        "text_validation_min",
        "text_validation_max",
        "identifier",
        "branching_logic",
        "required_field",
        "custom_alignment",
        "question_number",
        "matrix_group_name",
        "matrix_ranking",
        "field_annotation",
    ]
    keep_columns = [
        "form_name",
        "field_name",
        "field_type",
        "select_choices_or_calculations",
        "field_label",
        "text_validation_type_or_show_slider_number",
    ]
    addition_columns = [
        "status",
        "status questions",
        "exclude_reason",
        "notes",
        "ontology_term",
        "restrict_to_event_list",
    ]
    addition_df = pd.DataFrame(columns=addition_columns)

    column_order = [
        "form_name",
        "field_name",
        "status",
        "status questions",
        "exclude_reason",
        "notes",
        "field_type",
        "select_choices_or_calculations",
        "field_label",
        "text_validation_type_or_show_slider_number",
        "ontology_term",
        "restrict_to_event_list",
    ]

    extraction_df = pd.DataFrame(columns=init_columns)

    data_event = {
        "token": token,
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
    }
    response = requests.post(url, data=data_event)

    df = pd.json_normalize(response.json())

    extraction_df = pd.concat([extraction_df, df])
    extraction_df = extraction_df.drop(
        columns=list(set(init_columns) - set(keep_columns))
    )

    extraction_df = pd.concat([extraction_df, addition_df], axis=1)
    extraction_df = extraction_df[column_order]

    # copy in data from existing df that is in the addition column list (Status, etc)
    if existing_df is not None and not existing_df.empty:
        print("Updating existing df")
        extraction_df.set_index("field_name", inplace=True)
        if "status questions" not in existing_df.columns:
            existing_df["status questions"] = ""
        min_existing = existing_df[
            [
                "field_name",
                "status",
                "status questions",
                "notes",
                "restrict_to_event_list",
                "ontology_term",
            ]
        ].copy()
        min_existing.set_index("field_name", inplace=True)

        extraction_df.update(min_existing)
        extraction_df.reset_index(inplace=True)
        extraction_df = extraction_df[column_order]

    return extraction_df


# run like
# python dictionary_extraction.py <token> <existing csv> <new csv>
token = sys.argv[1]
existing_csv = sys.argv[2]
existing_df = pd.read_csv(existing_csv)


extraction_dictionary = dict_extract(existing_df)
extraction_dictionary.to_csv(sys.argv[3], index=False)
