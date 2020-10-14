import pandas as pd
import numpy as np
import dateutil
import datetime
from transform import REDCapETLTransform


class DateVariableTransform(REDCapETLTransform):
    data_namespace = 'TransformedDate'

    def __init__(self, etl):
        super().__init__(etl)
        transformdate_status_list = ['TransformDateYear', 'TransformDate', 'TransformDateTimeSeconds',
                                     'TransformDateTime']
        transformdate_field = pd.read_csv(self.etl.config.get('dcc_transforms', 'datetransform_fields_file'))
        self.transformdate_dict = transformdate_field[transformdate_field.status.isin(transformdate_status_list)]\
                                .set_index(['field_name'])\
                                .status.to_dict()
        #transformdate_field.groupby(['status']).field_name.apply(set).to_dict()['TransformDate']
        #transformdate_field[transformdate_field.status.isin(['TransformDateYear', 'TransformDate', 'TransformDateTimeSeconds', 'TransformDateTime'])].groupby(['status']).field_name.apply(set).to_dict()

    def process_records(self):
        if self.etl.config.get('dcc_transforms', 'datetransform_type') == 'total_seconds':
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


class TestCalcVariableTransform(REDCapETLTransform):
    data_namespace = 'CalcVars'

    def process_records(self):
        seen_record_ids = set()

        for record in self.etl.records:
            record_id = record.get('record')
            if record_id not in seen_record_ids:
                seen_record_ids.add(record_id)
                
                self.add_transform_record(record_id, 'calc_var_1', 1)
                self.add_transform_record(record_id, 'calc_var_2', 2)

        
        return True

    def get_transform_metadata(self):
        return [dict(field_name='calc_var_1', description='fake var 1'), dict(field_name='calc_var_2', description='fake var 2')]


class CalcVariableTransform(REDCapETLTransform):
    data_namespace = 'CalcVars'

    def __init__(self, etl):
        super().__init__(etl)
        self.deid_data = pd.read_csv(self.etl.config.get('dcc_transforms','deid_data_file'))
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


class TestRandomSecondaryIDTransform(REDCapETLTransform):
    data_namespace = 'SecondaryID'

    secondary_id_list = (505827,434820,377780,772844,559609,248224,950402,926616,740433,824441,528941,949635,581710,295895,206466,310511,895356,117734,248810,570347,116715,492749,924246,545853,448348,796848,712948,404466,796807,806600,610282,441134,877554,729403,808059,155970,843571,801514,335189,594093,614481,593285,572009,404612,954841,544348,772465,585373,856533,517696,110277,284547,336436,234572,962643,541513,486150,227686,519392,463268,613128,353759,110594,437916,693191,684061,260321,255137,695583,777945,815303,167545,631526,253290,919980,963858,850314,194239,763163,941525,415162,765513,552559,303503,584258,163260,558604,645970,632916,632202,256039,436156,143812,362033,146079,759862,202729,199313,867935,537676,451481,205987,832534,285152,457123,134653,676767,886015,990142,699074,639643,854520,421976,433467,194701,915421,308607,498001,489299,590681,155666,174346,599735,324180,380001,140206,354999,489402,480555,269214,751856,294104,868278,901940,656240,390614,308132,777720,535246,428963,814114,918826,539729,913921,981422,346425,399787,808501,541862,440554,854013,782961,230256,483976,205837,792186,983611,974072,407253,680105,361734,155530,953127,200573,710204,952568,849966,670621,725533,188616,302007,227896,254528,599624,859164,101764,697086,505079,412186,603956,526647,110055,482771,102638,113494,817211,242661,519702,109068,963337,992111,520724,409073,834152,604403,917234,582858,705683,977342,848763,998422,745088,696033,200119,183804,879228,154817,312749,472167,871634,251575,306203,642788,138406,993516,767245,147853,614772,519430,801483,451971,227039,506178,505154,202764,770192,936362,992021,547565,399858,402735,865321,898680,947039,756116,790228,604253,952325,867893,427140,802776,350624,287194,829698,897089,342722,778448,567932,402023,862824)

    def __init__(self, etl):
        
        self.used_list = set()
        
        super().__init__(etl)
            
    def get_secondary_id(self, record_id):
        for si in self.secondary_id_list:
            if si not in self.used_list:
                self.used_list.add(si)
                return si

    def process_records(self):

        seen_record_ids = set()
        for record in self.etl.records:
            record_id = record.get('record')
            if record_id not in seen_record_ids:
                secondary_id = self.get_secondary_id(record_id)
                seen_record_ids.add(record_id)
                self.add_transform_record(record_id, 'secondary_id', secondary_id)

        return True
        
    def get_transform_metadata(self):
        return [dict(field_name='secondary_id', description='Secondary unique identifier for use in public data set')]