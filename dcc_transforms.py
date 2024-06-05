import datetime
import logging

import dateutil
import pandas as pd
import pandera as pa

from transform import REDCapETLTransform


class DateVariableTransform(REDCapETLTransform):
    data_namespace = "TransformedDate"

    def __init__(self, etl):
        super().__init__(etl)
        transformdate_status_list = [
            "TransformDateYear",
            "TransformDate",
            "TransformDateTimeSeconds",
            "TransformDateTime",
        ]

        transformdate_field = self.etl.field_map.copy()

        self.transformdate_dict = (
            transformdate_field[
                transformdate_field.status.isin(transformdate_status_list)
            ]
            .set_index(["field_name"])
            .status.to_dict()
        )

    def process_records(self):
        transform_in_place = self.etl.config.getboolean(
            "dcc_transforms", "dob_shift_inplace", fallback=False
        )
        datetransform_type = self.etl.config.get(
            "dcc_transforms", "datetransform_type", fallback=None
        )
        if datetransform_type == "dob_shifting":
            anchor_date = dateutil.parser.isoparse(
                self.etl.config.get("dcc_transforms", "standard_date")
            )
            shift_dict = {
                record["record"]: anchor_date
                - dateutil.parser.isoparse(record["value"])
                for record in self.etl.records
                if record["field_name"] == "np_dob"
            }
            for record in self.etl.records:
                field_name = record.get("field_name")
                if self.transformdate_dict.get(field_name):
                    date_type = self.transformdate_dict.get(field_name)
                    original_value = record.get("value")
                    record_id = record.get("record")
                    originaldate = None
                    try:
                        originaldate = dateutil.parser.isoparse(original_value)
                    except ValueError:
                        logging.error(
                            f"dob_shifting: Failed to parse date: {original_value} "
                            f"record {record_id} field_name: {field_name}"
                        )

                    shift_interval = shift_dict.get(record_id)
                    if not shift_interval:
                        logging.error(
                            f"dob_shifting: No time shift defined for record {record_id}"
                        )
                    elif not originaldate:
                        logging.error(
                            f"dob_shifting: Failed to parse date: {original_value}"
                        )
                    else:
                        transformeddate = originaldate + shift_interval
                        # TODO - need to either add the rest of the data elements in record to these transforms
                        # OR transform the value in place and add a note that this occurred (my pref)
                        # we could then flag the record as transformed and capture any datelike
                        # data that has not been transformed in the phi filter
                        transformed_date = None

                        if date_type == "TransformDate":
                            transformed_date = transformeddate.date().isoformat()
                        elif date_type == "TransformDateTime":
                            transformed_date = (
                                transformeddate.date().isoformat()
                                + " "
                                + transformeddate.time().isoformat()[:-3]
                            )
                        elif date_type == "TransformDateTimeSeconds":
                            transformed_date = (
                                transformeddate.date().isoformat()
                                + " "
                                + transformeddate.time().isoformat()
                            )
                        elif date_type == "TransformDateYear":
                            transformed_date = transformeddate.date().isoformat()[:4]

                        if transformed_date:
                            if transform_in_place:
                                record["value"] = transformed_date
                                record["kpmp_date_cleaned"] = True
                                record["kpmp_date_cleaned_type"] = date_type

                            else:
                                self.add_transform_record(
                                    record_id=record_id,
                                    field_name=field_name,
                                    field_value=transformed_date,
                                )

                else:
                    continue
        elif datetransform_type == "total_seconds":
            standarddate = dateutil.parser.isoparse(
                self.etl.config.get("dcc_transforms", "standard_date")
            )
            for record in self.etl.records:
                field_name = record.get("field_name")
                if self.transformdate_dict.get(field_name):
                    originaldate = dateutil.parser.isoparse(record.get("value"))
                    transformeddate = int((standarddate - originaldate).total_seconds())
                    record_id = record.get("record")
                    self.add_transform_record(
                        record_id=record_id,
                        field_name=field_name,
                        field_value=transformeddate,
                    )

        elif datetransform_type == "date_shifting":
            shiftingseconds = datetime.timedelta(
                seconds=int(self.etl.config.get("dcc_transforms", "shifting_seconds"))
            )
            for record in self.etl.records:
                field_name = record.get("field_name")
                if self.transformdate_dict.get(field_name):
                    date_type = self.transformdate_dict.get(field_name)
                    originaldate = dateutil.parser.isoparse(record.get("value"))
                    transformeddate = originaldate + shiftingseconds
                    record_id = record.get("record")
                    if date_type == "TransformDate":
                        self.add_transform_record(
                            record_id=record_id,
                            field_name=field_name,
                            field_value=transformeddate.date().isoformat(),
                        )
                    elif date_type == "TransformDateTime":
                        self.add_transform_record(
                            record_id=record_id,
                            field_name=field_name,
                            field_value=transformeddate.date().isoformat()
                            + " "
                            + transformeddate.time().isoformat()[:-3],
                        )
                    elif date_type == "TransformDateTimeSeconds":
                        self.add_transform_record(
                            record_id=record_id,
                            field_name=field_name,
                            field_value=transformeddate.date().isoformat()
                            + " "
                            + transformeddate.time().isoformat(),
                        )
                    elif date_type == "TransformDateYear":
                        self.add_transform_record(
                            record_id=record_id,
                            field_name=field_name,
                            field_value=transformeddate.date().isoformat()[:4],
                        )
                else:
                    continue
        elif datetransform_type is None:
            logging.info("No datetransform active")
        else:
            raise NameError("Please enter a valid date transformation method.")

    def get_transform_metadata(self):
        if (
            self.etl.config.get("dcc_transforms", "datetransform_type", fallback=None)
            == "total_seconds"
        ):
            return [
                {"field_name": x[0], "granularity": x[1][9:]}
                for x in self.transformdate_dict.items()
            ]
        else:
            pass


class CalcVariableTransform(REDCapETLTransform):
    data_namespace = "CalcVars"

    def __init__(self, etl):
        super().__init__(etl)

        study_id_column_name = self.etl.config.get(
            "redcap", "study_id_column", fallback="study_id"
        )
        print(f"STUDY ID COL: {study_id_column_name}")
        usecols = [study_id_column_name]
        schema_to_check = None
        common_schema_cols = {
            "np_gender": pa.Column(str),
            "exp_age_decade": pa.Column(str),
            "exp_race": pa.Column(str),
            "mh_diabetes_yn": pa.Column(str),
            "exp_diabetes_duration": pa.Column(str),
            "mh_ht_yn": pa.Column(str),
            "exp_ht_duration": pa.Column(str),
        }
        usecols += list(common_schema_cols.keys())
        schema_to_check = common_schema_cols
        main_only_schema_cols = {
            "exp_disease_type": pa.Column(str),
            "exp_egfr_bl_cat": pa.Column(str),
            "exp_a1c_cat_most_recent": pa.Column(str),
            "exp_alb_cat_most_recent": pa.Column(str),
            "exp_pro_cat_most_recent": pa.Column(str),
            "exp_has_med_raas": pa.Column(str),
            "exp_aki_kdigo": pa.Column(str),
            "adj_primary_categoryC": pa.Column(str),
        }
        if self.etl.redcap_project_type == "KPMP_MAIN":
            usecols += list(main_only_schema_cols.keys())
            schema_to_check.update(main_only_schema_cols)

        self.deid_data = pd.read_csv(
            self.etl.config.get("dcc_transforms", "deid_data_file"),
            usecols=usecols,
            dtype=object,
        )
        if study_id_column_name != "redcap_id":
            self.deid_data.rename(columns={"study_id": "redcap_id"}, inplace=True)
        self.deid_data.fillna("", inplace=True)
        self.deid_data.set_index("redcap_id", inplace=True)

        # np_gender	exp_age_decade	exp_race	exp_disease_type	mh_diabetes_yn	exp_diabetes_duration
        # mh_ht_yn	exp_ht_duration	exp_egfr_bl_cat	exp_a1c_cat_most_recent	exp_alb_cat_most_recent
        # exp_pro_cat_most_recent	exp_has_med_raas	exp_aki_kdigo

        calc_schema = pa.DataFrameSchema(
            schema_to_check,
            index=pa.Index(str),
            strict=True,
        )
        calc_schema.validate(self.deid_data)
        # print(self.deid_data)

    def process_records(self):
        seen_record_ids = set()

        for record in self.etl.records:
            record_id = record.get("record_id")
            if record_id not in seen_record_ids:

                seen_record_ids.add(record_id)

                # secondary_id = self.etl.secondary_id_map.get(record_id)
                # if not secondary_id:
                #     print(f'no secondary_id for {record_id}')
                # else:
                #     print(f'got secondary_id {secondary_id} for {record_id}')
                if record_id in self.deid_data.index:
                    rec_deid_data = self.deid_data.loc[record_id]
                    for fk in rec_deid_data.keys():
                        if fk != "redcap_id":
                            fk_value = rec_deid_data[fk]

                            self.add_transform_record(record_id, fk, fk_value)

        return True

    def get_transform_metadata(self):
        self.deid_data_dictionary = pd.read_csv(
            self.etl.config.get("dcc_transforms", "deid_data_dictionary_file")
        )
        self.deid_data_dictionary.fillna("", inplace=True)

        return self.deid_data_dictionary.to_dict(orient="records")


class InterimSecondaryIDTransform(REDCapETLTransform):
    data_namespace = "SecondaryID"

    def __init__(self, etl):
        super().__init__(etl)
        secondary_id_mapping = pd.read_csv(
            self.etl.config.get("dcc_transforms", "secondary_id_file")
        )
        self.mapping_dict = secondary_id_mapping.set_index(["redcap_record_id"])[
            "secondary_id"
        ].to_dict()

    def get_secondary_id(self, record_id):
        sec_id = self.mapping_dict.get(record_id)
        return sec_id

    def process_records(self):

        seen_record_ids = set()
        for record in self.etl.records:
            record_id = record.get("record")
            if record_id not in seen_record_ids:
                secondary_id = self.get_secondary_id(record_id)
                seen_record_ids.add(record_id)
                self.add_transform_record(record_id, "secondary_id", secondary_id)
                self.etl.secondary_id_map[record_id] = secondary_id

        return True

    def get_transform_metadata(self):
        return [
            dict(
                field_name="secondary_id",
                description="Secondary unique identifier for use in public data set",
            )
        ]
