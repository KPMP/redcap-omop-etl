import requests
import re
import os
import sys
import datetime
import pandas as pd
import numpy as np

url = 'https://redcap.kpmp.org/api/'

def token_printer(auth_file):
    auth_regex = re.compile(r'(?<=TOKEN=)[0-9A-Za-z]+')
    f = open(auth_file, "r")
    token_line = f.readline()
    f.close()
    return auth_regex.search(token_line).group(0)


def dict_extract(existing_df):
    init_columns = ['field_name', 'form_name', 'section_header', 'field_type',
                    'field_label', 'select_choices_or_calculations', 'field_note',
                    'text_validation_type_or_show_slider_number', 'text_validation_min',
                    'text_validation_max', 'identifier', 'branching_logic',
                    'required_field', 'custom_alignment', 'question_number',
                    'matrix_group_name', 'matrix_ranking', 'field_annotation']
    keep_columns = ['form_name', 'field_name', 'field_type', 'select_choices_or_calculations',
                    'field_label', 'text_validation_type_or_show_slider_number']
    addition_columns = ['status', 'exclude_reason', 'notes', 'ontology_term', 'restrict_to_event_list']
    addition_df = pd.DataFrame(columns=addition_columns)


    column_order = ['form_name', 'field_name', 'status', 'exclude_reason', 'notes', 'field_type', 'select_choices_or_calculations',
                    'field_label', 'text_validation_type_or_show_slider_number','ontology_term', 'restrict_to_event_list']

    extraction_df = pd.DataFrame(columns=init_columns)
    
    data_event = \
        {
            'token': token,
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json',
        }
    response = requests.post(url, data=data_event)
    # header = response.headers
    df = pd.json_normalize(response.json())
    extraction_df = pd.concat([extraction_df, df])
    extraction_df = extraction_df.drop(columns=list(set(init_columns) - set(keep_columns)))
    
    extraction_df = pd.concat([extraction_df, addition_df], axis=1)
    extraction_df = extraction_df[column_order]

    # copy in data from existing df that is in the addition column list (Status, etc)
    if existing_df:
        extraction_df[addition_columns] = existing_df[existing_df['field_name'] == extraction_df['field_name']][addition_columns]
    
    return extraction_df


token = token_printer(sys.argv[1])
existing_csv = sys.argv[2]
existing_df = None
try:
    existing_df = pd.read_csv(existing_csv)
except pd.io.common.EmptyDataError:
    print("Empty existing csv passed as arg 2. Assuming we have no existing data to transfer")

extraction_dictionary = dict_extract(existing_df)
extraction_dictionary.to_csv(sys.argv[3], index=False)
