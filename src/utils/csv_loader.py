import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fastapi import UploadFile, HTTPException, status
import time
from dask import dataframe as df1
from tempfile import NamedTemporaryFile

from ..db import models
from ..utils import logger




logger = logger.get_logger("csv_loader")

BATCH_SIZE = 100000

async def load_store_status(db: Session, file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are allowed for store status upload."
        )
    
    total_inserted = 0
    try:
        s_time_dask = time.time()
        
        with NamedTemporaryFile(delete=False, mode='wb+') as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp.flush()
            tmp_path = tmp.name

        dask_df = df1.read_csv(tmp_path)

        e_time_dask = time.time()

        logger.info(f"CSV read time: {e_time_dask - s_time_dask:.2f} seconds")

        s_time_insert = time.time()
        total_inserted = 0
        for part in dask_df.partitions:
            df = part.compute()
            records = df.to_dict(orient="records")

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                stmt = pg_insert(models.StoreStatus.__table__).values(batch)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["store_id", "timestamp_utc"]
                )
                try:
                    db.execute(stmt)
                    db.commit()
                    total_inserted += len(batch)
                    logger.info(f"Processed batch {i // BATCH_SIZE + 1} with {len(batch)} records.")
                except Exception as e:
                    db.rollback()
                    logger.error(f"Batch insert failed at batch {i // BATCH_SIZE + 1}: {e}")
                    return {"status_code":status.HTTP_500_INTERNAL_SERVER_ERROR,"message": f"Database error during batch insert: {e}"}
                
        e_time_insert = time.time()
        logger.info(f"Batch insert time: {e_time_insert - s_time_insert:.2f} seconds")

        logger.info(f"Inserted/processed {total_inserted} records from Dask DataFrame.")
        return {"status_code": status.HTTP_200_OK ,"message": f"Successfully processed {total_inserted} records from {file.filename}."}

    except pd.errors.EmptyDataError:
        return {"status_code": status.HTTP_400_BAD_REQUEST ,"message": "CSV file is empty."}
    except pd.errors.ParserError as e:
         return {"status_code": status.HTTP_400_BAD_REQUEST ,"message": f"Error parsing CSV file: {e}"}

    except Exception as e:
        db.rollback()
        logger.error(f"An unexpected error occurred during CSV processing: {e}")
        return {"status_code": status.HTTP_500_INTERNAL_SERVER_ERROR ,"message": f"An unexpected error occurred: {e}"}

    finally:
        await file.close()

async def load_menu_hours(db: Session,file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are allowed for menu hour upload."
        )

    try:
        s_time_dask = time.time()
        with NamedTemporaryFile(delete=False, mode='wb+') as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp.flush()
            tmp_path = tmp.name
        ddf= df1.read_csv(tmp_path)
        e_time_dask = time.time()
        logger.info(f"CSV read time: {e_time_dask - s_time_dask:.2f} seconds")

        s_time_insert = time.time()
        total_inserted = 0

        for part in ddf.partitions:
            df = part.compute()

            records = df.to_dict(orient="records")

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                stmt = pg_insert(models.MenuHour.__table__).values(batch)
                
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["store_id", "dayOfWeek", "start_time_local", "end_time_local"]
                )

                try:
                    db.execute(stmt)
                    db.commit()
                    total_inserted += len(batch)
                    logger.info(f"Processed batch {i // BATCH_SIZE + 1} with {len(batch)} records.")
                except Exception as e:
                    db.rollback()
                    logger.error(f"Batch insert failed at batch {i // BATCH_SIZE + 1}: {e}")
                    return {"status_code": status.HTTP_500_INTERNAL_SERVER_ERROR ,"message": f"Database error during batch insert: {e}"}

        e_time_insert = time.time()
        logger.info(f"Batch insert time: {e_time_insert - s_time_insert:.2f} seconds")
        return {"status_code": status.HTTP_200_OK ,"message": f"Successfully processed {total_inserted} menu hour records from {file.filename}."}

        
    except Exception as e:
        db.rollback()
        logger.error(f"An unexpected error occurred during CSV processing: {e}")
        return {"status_code": status.HTTP_500_INTERNAL_SERVER_ERROR ,"message": f"An unexpected error occurred: {e}"}

    finally:
        await file.close()

async def load_timezones(db: Session, file: UploadFile):
    if not file.filename.endswith(".csv"):
        return {"status_code": status.HTTP_400_BAD_REQUEST ,"message": "Only CSV files are allowed for timezone upload."}

    try:
        s_time_dask = time.time()
        with NamedTemporaryFile(delete=False, mode='wb+') as tmp:
            contents = await file.read()
            tmp.write(contents)
            tmp.flush()
            tmp_path = tmp.name
        ddf= df1.read_csv(tmp_path)
        e_time_dask = time.time()
        logger.info(f"CSV read time: {e_time_dask - s_time_dask:.2f} seconds")

        s_time_insert = time.time()
        total_inserted = 0

        for part in ddf.partitions:
            df = part.compute()

            records = df.to_dict(orient="records")

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                stmt = pg_insert(models.Timezone.__table__).values(batch)
                
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["store_id", "timezone_str"]
                )

                try:
                    db.execute(stmt)
                    db.commit()
                    total_inserted += len(batch)
                    logger.info(f"Processed batch {i // BATCH_SIZE + 1} with {len(batch)} records.")
                except Exception as e:
                    db.rollback()
                    logger.error(f"Batch insert failed at batch {i // BATCH_SIZE + 1}: {e}")
                    return {"status_code": status.HTTP_500_INTERNAL_SERVER_ERROR ,"message": f"Database error during batch insert: {e}"}

        e_time_insert = time.time()
        logger.info(f"Batch insert time: {e_time_insert - s_time_insert:.2f} seconds")
        return {"status_code": status.HTTP_200_OK ,"message": f"Successfully processed {total_inserted} timezone records from {file.filename}."}

        
    except Exception as e:
        db.rollback()
        logger.error(f"An unexpected error occurred during CSV processing: {e}")
        return {"status_code": status.HTTP_500_INTERNAL_SERVER_ERROR ,"message": f"An unexpected error occurred: {e}"}

    finally:
        await file.close()

    