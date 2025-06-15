from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session
from uuid import uuid4
from fastapi.responses import FileResponse

from ..utils import logger
from ..schema import report as report_schema
from ..service import report_generator,report
from ..db import session,models


logger = logger.get_logger("report_endpoint")


router = APIRouter()

def get_db():
    db = session.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/trigger_report", response_model=report_schema.TriggerReportResponse)
def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    
    logger.info("Triggering report generation")
    
    report_id = str(uuid4())
    report.create_report_entry(db, report_id)
    
    background_tasks.add_task(report_generator.generate_report, report_id,db)
    return {"report_id": report_id}

@router.get("/get_report", response_model=report_schema.ReportStatusResponse)
def get_report(report_id: str, db: Session = Depends(get_db)):
    report = db.query(models.Report).filter(models.Report.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    if report.status != "Complete":
        return {"status": "Running"}
    
    return {
        "status": "Complete",
        "report_url": f"/download_report/{report_id}"
    }

@router.get("/download_report/{report_id}")
def download_report(report_id: str, db: Session = Depends(get_db)):
    report = db.query(models.Report).filter(models.Report.id == report_id).first()
    if not report or report.status != "Complete":
        raise HTTPException(status_code=404, detail="Report not ready")

    return FileResponse(report.file_path, media_type='text/csv', filename=f"{report_id}.csv")