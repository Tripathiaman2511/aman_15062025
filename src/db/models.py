from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(String)
    status = Column(String) 

    __table_args__ = (
        UniqueConstraint("store_id", "timestamp_utc", name="uix_store_timestamp"),
    )

class MenuHour(Base):
    __tablename__ = "menu_hour"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    dayOfWeek = Column(Integer)
    start_time_local = Column(String,default="00:00:00")
    end_time_local = Column(String, default="23:59:59")

    __table_args__ = (
        UniqueConstraint("store_id", "dayOfWeek","start_time_local","end_time_local", name="uix_menu_timestamp"),
    )

class Timezone(Base):
    __tablename__ = "timezone"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, unique=True)
    timezone_str = Column(String,default="America/Chicago")
    __table_args__ = (
        UniqueConstraint("store_id", "timezone_str", name="uix_store_timezone"),
    )

class Report(Base):
    __tablename__ = "report"
    id = Column(String, primary_key=True, index=True)
    status = Column(String)  # "Running" or "Complete"
    created_at = Column(DateTime)
    file_path = Column(String, nullable=True)