# Project Overview

This project is designed to help restaurant owners monitor their store availability and generate uptime/downtime reports based on historical operational data. The service is tailored for businesses across the United States and focuses on identifying periods when a store was unexpectedly offline during its designated business hours.
## API Endpoints

| Method | Endpoint        | Description           |
|--------|----------------|----------------------|
| POST    | `/trigger_report`   | trigger report generation from the data provided       |
| GET    | `/get_report` |  return the status of the report or the url to dowmload csv   | 
| POST   | `/download_report/{report_id}`   | download the csv file    |
| PUT    | `load_data` | load csv data for report generation     |

## Installation

```bash
git clone https://github.com/Tripathiaman2511/aman_15062025.git
cd aman_15062025
python3 -m venv venv
pip install -r requirements.txt
```

## Running the Application

```bash
docker compose up --build
```

## Setting up .env file

```bash
DATABASE_URL=
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
DB_PORT=
```

The server will start on `http://localhost:8000`. For FastAPI Swagger UI : `http://localhost:8000/docs` 


## Area for improvement

- Refactor the logic to compute uptime/downtime for a single store_id into a separate function to enable parallel processing, modular testing, and easier scaling. 
- Optimize data processing by pre-filtering relevant time ranges in SQL queries, caching timezone conversions, and using vectorized pandas operations to reduce per-store loop overhead.
