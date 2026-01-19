from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from io import StringIO
from database import get_connection
import logging
from users import upload_users
import csv
import io
from datetime import datetime
from loguru import logger
#
# from users import get_users

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s | %(pathname)s:%(lineno)d',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("my_fastapi_app")

logger.info("This is a custom formatted log")

logger.info("Starting FastAPI application")

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://myfrontend.com"],
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"]
)


@app.get("/api/data")
def read_data():
    logger.info("CORS test endpoint called")
    return {"message": "CORS works!"}
    logger .info("CORS test endpoint called")

@app.get("/items/{item_id}")
async def read_item(item_id):
    return {"item_id": item_id}
logger.info("Read item endpoint called")
@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    # Validate file type
    if not file.filename.endswith(".csv"):
        logger.error(f"Invalid file type attempted: {file.filename}")
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    # Read CSV bytes → string
    file_bytes = await file.read()
    file_str = file_bytes.decode("utf-8")

    # Use pandas to parse CSV
    df = pd.read_csv(StringIO(file_str))

    # Convert to JSON response
    data = df.to_dict(orient="records")
    logger.info(f"Uploaded CSV file: {file.filename} with {len(data)} rows")
    return {
        "filename": file.filename,
        "rows": len(data),
        "data": data
    }


conn = get_connection()
if conn:
    print("✅ Successfully connected to the database")
else:
    print("❌ Failed to connect to the database")

async def root():
    return {"message": "Welcome to the CSV upload API. Use the /upload-csv endpoint to upload a CSV file."}
app.get("/")(root)



@app.post("/users/upload-csv")
async def upload_users_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    contents = await file.read()
    csv_stream = io.StringIO(contents.decode("utf-8"))
    reader = csv.DictReader(csv_stream)

    required_cols = {"phone_number", "full_names"}
    if not required_cols.issubset(reader.fieldnames):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain {required_cols}"
        )

    users = []
    skipped = 0

    for row in reader:
        try:
            dob = None
            if row.get("date_of_birth"):
                dob = datetime.strptime(row["date_of_birth"], "%Y-%m-%d").date()

            users.append({
                "phone_number": row["phone_number"].strip(),
                "full_names": row["full_names"].strip(),
                "email": row.get("email"),
                "national_id_number": row.get("national_id_number"),
                "date_of_birth": dob,
            })
        except Exception as e:
            logger.warning(f"Skipping row {row}: {e}")
            skipped += 1

    if not users:
        raise HTTPException(status_code=400, detail="No valid rows found")

    # result = await bulk_upsert_users(users)
    result = {"inserted": 0, "updated": 0}


    return {
        "uploaded": len(users),
        "inserted": result["inserted"],
        "updated": result["updated"],
        "skipped": skipped
    }
