from pydantic import BaseModel
from typing import Optional

class TriggerReportResponse(BaseModel):
    report_id: str

class ReportStatusResponse(BaseModel):
    status: str
    report_url: Optional[str] = None

class LoadDataResponse(BaseModel):
    load_store_status: Optional[dict] = None
    load_menu_hours: Optional[dict] = None
    load_timezones: Optional[dict] = None
    