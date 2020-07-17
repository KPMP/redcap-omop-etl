import requests
import re
import sys
import datetime
import pandas as pd
import numpy as np

url = 'https://redcap.kpmp.org/api/'

field_table = pd.read_csv(sys.argv[1])
token = token_printer(sys.argv[2])
form_field_dict = field_table.groupby('Form').Field.apply(list).to_dict()
form_event_dict = field_table.groupby('Form').Event.apply(lambda x: list(set(x))[0]).to_dict()


def token_printer(auth_file):
    auth_regex = re.compile(r'(?<=TOKEN=)[0-9A-Za-z]+')
    f = open(auth_file, "r")
    token_line = f.readline()
    f.close()
    return auth_regex.search(token_line).group(0)


def dict_extract(form_event_map):
    init_columns = ['field_name', 'form_name', 'section_header', 'field_type',
                    'field_label', 'select_choices_or_calculations', 'field_note',
                    'text_validation_type_or_show_slider_number', 'text_validation_min',
                    'text_validation_max', 'identifier', 'branching_logic',
                    'required_field', 'custom_alignment', 'question_number',
                    'matrix_group_name', 'matrix_ranking', 'field_annotation']
    keep_columns = ['field_name', 'form_name', 'field_type', 'select_choices_or_calculations',
                    'field_label', 'text_validation_type_or_show_slider_number']
    addition_df = pd.DataFrame(columns=['status', 'exclude_reason', 'notes', 'ontology_term', 'restrict_to_event_list'])
    extraction_df = pd.DataFrame(columns=init_columns)
    for i, k in enumerate(form_event_map.keys()):
        data_event = \
            {
                'token': token,
                'content': 'metadata',
                'format': 'json',
                'returnFormat': 'json',
                f'forms[{i}]': k
            }
        response = requests.post(url, data=data_event)
        # header = response.headers
        df = pd.json_normalize(response.json())
        extraction_df = pd.concat([extraction_df, df])
    extraction_df = extraction_df.drop(columns=list(set(init_columns) - set(keep_columns)))
    extraction_df = pd.concat([extraction_df, addition_df], axis=1)
    return extraction_df


extraction_dictionary = dict_extract(form_event_dict)
extraction_dictionary.to_csv(sys.argv[3], index=False)
