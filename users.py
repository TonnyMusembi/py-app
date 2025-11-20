from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi import FastAPI
import pandas as pd

selected_columns = ['id', 'name', 'email']
from io import StringIO
from database import get_connection

async def upload_users(file: UploadFile = File(...)):
    try:
        # Read the uploaded file
        file_content = await file.read()
        df = pd.read_csv(StringIO(file_content.decode('utf-8')))

        # Select specific columns
        df = df[selected_columns]

        # Connect to MySQL
        conn = get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to connect to MySQL")

        cursor = conn.cursor()

        # Insert data into MySQL
        for _, row in df.iterrows():
            sql = "INSERT INTO users (id, name, email) VALUES (%s, %s, %s)"
            cursor.execute(sql, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Users uploaded successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



app = FastAPI()
