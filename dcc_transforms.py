import pandas as pd
import numpy as np
import dateutil
import datetime
import logging
from transform import REDCapETLTransform

class DateVariableTransform(REDCapETLTransform):
    data_namespace = 'TransformedDate'

    def __init__(self, etl):
        super().__init__(etl)
        transformdate_status_list = ['TransformDateYear', 'TransformDate', 'TransformDateTimeSeconds',
                                     'TransformDateTime']
        #transformdate_field = pd.read_csv(self.etl.config.get('dcc_transforms', 'datetransform_fields_file'))
        transformdate_field = self.etl.field_map.copy()

        self.transformdate_dict = transformdate_field[transformdate_field.status.isin(transformdate_status_list)]\
                                .set_index(['field_name'])\
                                .status.to_dict()
        #transformdate_field.groupby(['status']).field_name.apply(set).to_dict()['TransformDate']
        #transformdate_field[transformdate_field.status.isin(['TransformDateYear', 'TransformDate', 'TransformDateTimeSeconds', 'TransformDateTime'])].groupby(['status']).field_name.apply(set).to_dict()

    def process_records(self):
        transform_in_place = self.etl.config.getboolean('dcc_transforms', 'dob_shift_inplace', fallback=False)
        if self.etl.config.get('dcc_transforms', 'datetransform_type') == 'dob_shifting':
            anchor_date = dateutil.parser.isoparse(self.etl.config.get('dcc_transforms', 'standard_date'))
            shift_dict = {record['record']: anchor_date - dateutil.parser.isoparse(record['value'])
                          for record in self.etl.records if record['field_name'] == 'np_dob'}
            for record in self.etl.records:
                field_name = record.get('field_name')
                if self.transformdate_dict.get(field_name):
                    date_type = self.transformdate_dict.get(field_name)
                    original_value = record.get('value')
                    record_id = record.get('record')
                    originaldate = None
                    try:
                        originaldate = dateutil.parser.isoparse(original_value)
                    except ValueError:
                        logging.error(f'dob_shifting: Failed to parse date: {original_value} record {record_id} field_name: {field_name}')

                    
                    shift_interval = shift_dict.get(record_id)
                    if not shift_interval:
                        logging.error(f'dob_shifting: No time shift defined for record {record_id}')
                    elif not originaldate:
                        logging.error(f'dob_shifting: Failed to parse date: {original_value}')
                    else:
                        transformeddate = originaldate + shift_interval
                        # TODO - need to either add the rest of the data elements in record to these transforms 
                        # OR transform the value in place and add a note that this occurred (my pref)
                        # we could then flag the record as transformed and capture any datelike 
                        # data that has not been transformed in the phi filter
                        transformed_date = None

                        if date_type == 'TransformDate':
                            transformed_date = transformeddate.date().isoformat()
                        elif date_type == 'TransformDateTime':
                            transformed_date = transformeddate.date().isoformat()\
                                                                + ' '\
                                                                + transformeddate.time().isoformat()[:-3]
                        elif date_type == 'TransformDateTimeSeconds':
                            transformed_date = transformeddate.date().isoformat()\
                                                                + ' '\
                                                                + transformeddate.time().isoformat()
                        elif date_type == 'TransformDateYear':
                            transformed_date = transformeddate.date().isoformat()[:4]
                        
                        if transformed_date:
                            if transform_in_place:
                                record['value'] = transformed_date
                                record['kpmp_date_cleaned'] = True
                                record['kpmp_date_cleaned_type'] = date_type
                                if field_name == 'np_dob':
                                    record['kpmp_orig'] = original_value
                            else:
                                self.add_transform_record(record_id=record_id,
                                                        field_name=field_name,
                                                        field_value=transformed_date)

                else:
                    continue
        elif self.etl.config.get('dcc_transforms', 'datetransform_type') == 'total_seconds':
            standarddate = dateutil.parser.isoparse(self.etl.config.get('dcc_transforms', 'standard_date'))
            for record in self.etl.records:
                field_name = record.get('field_name')
                if self.transformdate_dict.get(field_name):
                    originaldate = dateutil.parser.isoparse(record.get('value'))
                    transformeddate = int((standarddate - originaldate).total_seconds())
                    record_id = record.get('record')
                    self.add_transform_record(record_id=record_id, field_name=field_name, field_value=transformeddate)

        elif self.etl.config.get('dcc_transforms', 'datetransform_type') == 'date_shifting':
            shiftingseconds = datetime.timedelta(seconds=int(self.etl.config.get('dcc_transforms', 'shifting_seconds')))
            for record in self.etl.records:
                field_name = record.get('field_name')
                if self.transformdate_dict.get(field_name):
                    date_type = self.transformdate_dict.get(field_name)
                    originaldate = dateutil.parser.isoparse(record.get('value'))
                    transformeddate = originaldate + shiftingseconds
                    record_id = record.get('record')
                    if date_type == 'TransformDate':
                        self.add_transform_record(record_id=record_id,
                                                  field_name=field_name,
                                                  field_value=transformeddate.date().isoformat())
                    elif date_type == 'TransformDateTime':
                        self.add_transform_record(record_id=record_id,
                                                  field_name=field_name,
                                                  field_value=transformeddate.date().isoformat()
                                                              + ' '
                                                              + transformeddate.time().isoformat()[:-3])
                    elif date_type == 'TransformDateTimeSeconds':
                        self.add_transform_record(record_id=record_id,
                                                  field_name=field_name,
                                                  field_value=transformeddate.date().isoformat()
                                                              + ' '
                                                              + transformeddate.time().isoformat())
                    elif date_type == 'TransformDateYear':
                        self.add_transform_record(record_id=record_id,
                                                  field_name=field_name,
                                                  field_value=transformeddate.date().isoformat()[:4])
                else:
                    continue

        else:
            raise NameError('Please enter a valid date transformation method.')

    def get_transform_metadata(self):
        if self.etl.config.get('dcc_transforms', 'datetransform_type') == 'total_seconds':
            return [{'field_name': x[0], 'granularity': x[1][9:]} for x in self.transformdate_dict.items()]
        else:
            pass


class CalcVariableTransform(REDCapETLTransform):
    data_namespace = 'CalcVars'

    def __init__(self, etl):
        super().__init__(etl)
        self.deid_data = pd.read_csv(self.etl.config.get('dcc_transforms', 'deid_data_file'))
        self.deid_data.fillna('', inplace=True)
        self.deid_data.set_index('exp_part_uniq_id', inplace=True)
        #print(self.deid_data)
        
    def process_records(self):
        seen_record_ids = set()

        for record in self.etl.records:
            record_id = record.get('record')
            if record_id not in seen_record_ids:
                
                seen_record_ids.add(record_id)

                secondary_id = self.etl.secondary_id_map.get(record_id)
                # if not secondary_id:
                #     print(f'no secondary_id for {record_id}')
                # else:
                #     print(f'got secondary_id {secondary_id} for {record_id}')
                if secondary_id in self.deid_data.index:
                    rec_deid_data = self.deid_data.loc[secondary_id]
                    for fk in rec_deid_data.keys():
                        if fk != 'redcap_id':
                            fk_value = rec_deid_data[fk]
                            
                            self.add_transform_record(record_id, fk, fk_value)

        return True

    def get_transform_metadata(self):
        self.deid_data_dictionary = pd.read_csv(self.etl.config.get('dcc_transforms', 'deid_data_dictionary_file'))
        self.deid_data_dictionary.fillna('', inplace=True)

        return self.deid_data_dictionary.to_dict(orient='records')
        #[dict(field_name='calc_var_1', description='fake var 1'), dict(field_name='calc_var_2', description='fake var 2')]


class InterimSecondaryIDTransform(REDCapETLTransform):
    data_namespace = 'SecondaryID'

    def __init__(self, etl):
        super().__init__(etl)
        secondary_id_mapping = pd.read_csv(self.etl.config.get('dcc_transforms','secondary_id_file'))
        self.mapping_dict = secondary_id_mapping.set_index(['redcap_record_id'])['secondary_id'].to_dict()
        
    
    def get_secondary_id(self, record_id):
        sec_id = self.mapping_dict.get(record_id)
        return sec_id

    def process_records(self):

        seen_record_ids = set()
        for record in self.etl.records:
            record_id = record.get('record')
            if record_id not in seen_record_ids:
                secondary_id = self.get_secondary_id(record_id)
                seen_record_ids.add(record_id)
                self.add_transform_record(record_id, 'secondary_id', secondary_id)
                self.etl.secondary_id_map[record_id] = secondary_id

        return True

    def get_transform_metadata(self):
        return [dict(field_name='secondary_id', description='Secondary unique identifier for use in public data set')]
