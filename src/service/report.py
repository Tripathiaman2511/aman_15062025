from sqlalchemy.orm import Session
from datetime import datetime

from ..db import models


def create_report_entry(db: Session, report_id: str):
    report = models.Report(id=report_id, status="Running", created_at=datetime.utcnow())
    db.add(report)
    db.commit()
    db.refresh(report)
    return report

def update_report_status(db: Session, report_id: str, file_path: str,status: str = "Complete"):
    report = db.query(models.Report).filter(models.Report.id == report_id).first()
    if report:
        report.status = status
        report.file_path = file_path
        db.commit()