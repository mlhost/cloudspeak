import datetime
import pandas as pd


def now():
    return pd.to_datetime("now").tz_localize("utc")


def to_datetime(dt, to_current_timezone=True):
    dt = pd.to_datetime(dt)

    if to_current_timezone:
        tz = datetime.datetime.now().astimezone().tzinfo
        dt = dt.tz_convert(tz)

    return dt
