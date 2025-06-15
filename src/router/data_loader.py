from fastapi import APIRouter, Depends, UploadFile

from ..utils import csv_loader
from ..db import session
from ..schema import report as report_schema

router = APIRouter()

def get_db():
    db = session.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/load_data",response_model=report_schema.LoadDataResponse)
async def load_csv_data(db = Depends(get_db),file1:UploadFile = None,file2:UploadFile = None,file3:UploadFile = None):
    
    try:
        load_store_response=await csv_loader.load_store_status(db,file1) if file1 else {"message": "No file provided for store status"}
        load_menu_response=await csv_loader.load_menu_hours(db, file2) if file2 else {"message": "No file provided for menu hours"}
        load_timezone_response = await csv_loader.load_timezones(db, file3) if file3 else {"message": "No file provided for timezones"}
        return {
            "load_store_status": load_store_response,
            "load_menu_hours": load_menu_response,
            "load_timezones": load_timezone_response
        }
    except Exception as e:
        return {"error": str(e)}