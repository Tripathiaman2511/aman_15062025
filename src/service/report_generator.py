import os
import csv
from sqlalchemy.orm import Session
import pandas as pd
import time
import pytz
from datetime import datetime

from . import report as report_crud
from ..db import models
from ..utils import time_utils,logger


logger = logger.get_logger("report_generator")

def generate_report(report_id: str,db: Session):
    try:
        s_time_dask = time.time()
        results = compute_uptime_downtime(db) # computing uptime and downtime of stores
        e_time_dask = time.time()
        logger.info(f"Report generation time: {e_time_dask - s_time_dask:.2f} seconds")

        output_path = f"./reports/{report_id}.csv"
        os.makedirs("./reports", exist_ok=True)

        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "store_id", 
                "uptime_last_hour", "uptime_last_day", "uptime_last_week",
                "downtime_last_hour", "downtime_last_day", "downtime_last_week"
            ])
            writer.writeheader()
            writer.writerows(results)

        report_crud.update_report_status(db, report_id, output_path)
    except Exception as e:
        logger.error(f"Error generating report {report_id}: {e}")
        report_crud.update_report_status(db, report_id, None, status="Failed")
    finally:
        db.close()

def compute_uptime_downtime(db:Session):
    """
    Compute the uptime and downtime for each store over the last hour, last day, and last week.
    It considers only business hours as defined in menu_hours (with timezone support).
    """
    try:
        # 1. Get all distinct store IDs from status data
        store_ids = db.query(models.StoreStatus.store_id).distinct() #getting all storeid
        
        # 2. Get the latest timestamp across all store statuses (assumed to be current reference time) and convert it into UTC datetime object
        current_time = db.query(models.StoreStatus.timestamp_utc).order_by(models.StoreStatus.timestamp_utc.desc()).first()
        timestamp_in_utc=pytz.utc.localize(datetime.strptime(current_time.timestamp_utc, '%Y-%m-%d %H:%M:%S.%f UTC'))
        current_date=timestamp_in_utc.date()

        logger.info(f"Current timestamp: {timestamp_in_utc if current_time else 'No data available'}")

        # 4. Iterate over each store
        results = [] 
        for (store_id,) in store_ids:

            # 4.1. Get store's timezone; default to 'America/Chicago' if missing
            timezone_row = db.query(models.Timezone).filter(models.Timezone.store_id == store_id).first()
            tz = timezone_row.timezone_str if timezone_row else "America/Chicago"
            
            # 4.2. Get menu hours (i.e., business hours) for the store
            menu_rows = db.query(models.MenuHour).filter(models.MenuHour.store_id == store_id).all()

            # 4.3. Build DataFrame of menu hours in UTC
            menu_df =pd.DataFrame([{
                "day_of_week": m.dayOfWeek,
                "start_time_local": pytz.timezone(tz).localize(datetime.combine(current_date,datetime.strptime(m.start_time_local,'%H:%M:%S').time())).astimezone(pytz.utc),
                "end_time_local": pytz.timezone(tz).localize(datetime.combine(current_date,datetime.strptime(m.end_time_local,'%H:%M:%S').time())).astimezone(pytz.utc)
            } for m in menu_rows])  if menu_rows else  pd.DataFrame([{
                "day_of_week": i, 
                "start_time_local": pytz.timezone(tz).localize(datetime.combine(current_date, datetime.strptime("00:00:00", '%H:%M:%S').time())).astimezone(pytz.utc),
                "end_time_local": pytz.timezone(tz).localize(datetime.combine(current_date, datetime.strptime("23:59:59", '%H:%M:%S').time())).astimezone(pytz.utc)
            } for i in range(7)])

            logger.info(f"Processing store {store_id} with timezone {tz} menu hours:\n{menu_df.to_string()}")

            # 4.4. Get business hours windows for past hour/day/week in UTC
            windows = time_utils.get_operating_intervals_within_window(menu_df, tz,timestamp_in_utc)

            # 4.5. Load store status logs and convert to DataFrame
            status_records = db.query(models.StoreStatus).filter(models.StoreStatus.store_id == store_id).order_by(models.StoreStatus.timestamp_utc)

            if not status_records:
                logger.info(f"No status data for store {store_id}, skipping.")
                continue

            df = pd.DataFrame([{
                "timestamp": pytz.utc.localize(datetime.strptime(s.timestamp_utc, '%Y-%m-%d %H:%M:%S.%f UTC')),
                "status": s.status
            } for s in status_records])
            

            logger.info(f"Top 5 records:\n{df.head(5)}")

            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df = df.set_index('timestamp')
            logger.info(f"df index: {df.index.name}")

             # 4.6. Interpolation logic: calculate active/inactive time in each window
            interpolated_results = {
                "store_id": store_id,
                "uptime_last_hour": 0, "uptime_last_day": 0, "uptime_last_week": 0,
                "downtime_last_hour": 0, "downtime_last_day": 0, "downtime_last_week": 0,
            }

            for key in ["last_hour", "last_day", "last_week"]:
                uptime, downtime = 0, 0

                for window_start, window_end in windows.get(key, []):
                    time_range  = pd.date_range(start=window_start, end=window_end, freq="5min")
                    logger.info(f"time_range: {time_range}")
                    if time_range.empty:
                        continue
                    interpolated  = df.reindex(time_range, method="ffill")
                    uptime += (interpolated["status"] == "active").sum() * 5
                    downtime += (interpolated["status"] == "inactive").sum() * 5

                if "hour" in key:
                    interpolated_results[f"uptime_{key}"] = uptime
                    interpolated_results[f"downtime_{key}"] = downtime
                else:
                    interpolated_results[f"uptime_{key}"] = round(uptime / 60, 2)
                    interpolated_results[f"downtime_{key}"] = round(downtime / 60, 2)
                
                logger.info(f"interpolated_results: {interpolated_results}")
            
            results.append(interpolated_results)
    except Exception as e:
        logger.error(f"Error while compute_uptime_downtime: {e} \ncurrent result: {results}")
        raise e
    return results