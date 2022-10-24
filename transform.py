from abc import ABC, abstractmethod


class REDCapETLTransform(ABC):
    """
    Base class for transforms. Not doing much yet but will as we build out.
    """

    data_namespace = None

    def __init__(self, etl):
        self.transform_records = []
        self.etl = etl
        assert self.data_namespace is not None

    def add_transform_record(self, record_id, field_name, field_value):
        self.transform_records.append(
            dict(
                record_id=record_id,
                namespace=self.data_namespace,
                field_name=field_name,
                field_value=field_value,
            )
        )

    def get_transform_records(self):
        return self.transform_records

    @abstractmethod
    def process_records(self):
        """
        Takes ETL app object, returns success/failure
        [{}]
        """
        pass

    @abstractmethod
    def get_transform_metadata(self):
        pass
