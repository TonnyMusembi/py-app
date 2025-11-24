from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)
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

        logging.info("Users uploaded successfully")
        return {"message": "Users uploaded successfully"}

    except Exception as e:
        logging.error("Failed to upload users: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    async def get_users():
        try:
            conn = get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Failed to connect to MySQL")
                logging.info("Users fetched successfully")

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users")
            users = cursor.fetchall()
            cursor.close()
            conn.close()
            logging.info("Users fetched successfully")
            return users

        except Exception as e:
            logging.error("Failed to get users: %s", e)
            raise HTTPException(status_code=500, detail=str(e))
