from typing import Dict, List, Tuple
import pandas as pd
from datetime import datetime,timedelta
import pytz

from . import logger

logger = logger.get_logger("time_utils")

def get_operating_intervals_within_window(menu_df:pd.DataFrame, store_tz:str,timestamp_in_utc:datetime)->Dict[str, List[Tuple[datetime, datetime]]]:

    """
    Computes operating intervals (in UTC) for the past hour, day, and week
    based on store business hours and time zone.

    Args:
        menu_df (pd.DataFrame): DataFrame containing store's weekly menu hours.
                                Columns: day_of_week, start_time_local, end_time_local (in UTC).
        store_tz (str): Time zone string of the store (e.g., 'America/Chicago').
        timestamp_in_utc (datetime): Current UTC timestamp to calculate windows backward from.

    Returns:
        Dict[str, List[Tuple[datetime, datetime]]]: Dictionary with keys
        'last_hour', 'last_day', and 'last_week'. Each key maps to a list of
        tuples (start_datetime_utc, end_datetime_utc) representing business hours.
    """

    try:
    
        logger.info(f"Calculating business windows for store timezone: {store_tz} at local time: {timestamp_in_utc}")

        time_windows = {
            "last_hour": (timestamp_in_utc - timedelta(hours=1),timestamp_in_utc),
            "last_day": (timestamp_in_utc - timedelta(days=1),timestamp_in_utc),
            "last_week": (timestamp_in_utc - timedelta(days=7),timestamp_in_utc),
        }

        logger.info(f"Business windows calculated: {time_windows}")

        intervals = {"last_hour": [], "last_day": [], "last_week": []}
        

        for window_name, (start_utc, end_utc) in time_windows.items():
            for _, row in menu_df.iterrows():
                day_of_week = row["day_of_week"]
                start_local = row["start_time_local"]
                end_local = row["end_time_local"]

                current_day = end_utc.date()
                while current_day >= start_utc.date():
                    if current_day.weekday() == day_of_week:
                        start_dt = pytz.timezone(store_tz).localize(datetime.combine(current_day, start_local.time())).astimezone(pytz.utc)
                        end_dt = pytz.timezone(store_tz).localize(datetime.combine(current_day, end_local.time())).astimezone(pytz.utc)
                        
                        # Clip to window boundaries
                        clipped_start = max(start_dt, start_utc)
                        clipped_end = min(end_dt, end_utc)
                        
                        if clipped_start < clipped_end:
                            intervals[window_name].append((clipped_start, clipped_end))
                    current_day -= timedelta(days=1)
        logger.info(f"Business intervals for {store_tz}: {intervals}")
    except Exception as e:
        logger.error(f"Error generating business intervals {store_tz}: {e}")
        raise e
    return intervals