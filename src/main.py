#entry point
from fastapi import FastAPI

from .router import report_endpoint, data_loader
from .db import session,models


models.Base.metadata.create_all(bind=session.engine)

app=FastAPI()
app.include_router(report_endpoint.router) #endpoint for generating report
app.include_router(data_loader.router) # endpoint for loading csv data
