import datetime


def time_transform(some_datetime, init_datetime):
    return (
        datetime.datetime(some_datetime) - datetime.datetime(init_datetime)
    ).total_seconds()
