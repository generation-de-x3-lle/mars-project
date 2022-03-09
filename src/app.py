import psycopg2
import os
from dotenv import load_dotenv





def connect_to_db():
# Load environment variables from .env file
    load_dotenv()
    host = os.environ.get("POSTGRES_HOST")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    database = os.environ.get("POSTGRES_DB")

# Establish a database connection

    connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

    return connection

