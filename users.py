from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd

selected_columns = ['id', 'name', 'email']
from io import StringIO
from database import get_connection
