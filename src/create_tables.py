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

connection =  connect_to_db()
connection.autocommit = True
cursor = connection.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS Transactions (
        transaction_id  SERIAL PRIMARY KEY, 
        transaction_date Date, 
        transaction_time TIME, 
        branch_name VARCHAR(100), 
        total_amount REAL, 
        payment_method VARCHAR(100)  
    );
    """
)
connection.commit()
cursor.close()

cursor = connection.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS products (
        product_id  SERIAL PRIMARY KEY, 
        item_size VARCHAR (50),
        item_name VARCHAR (50),
        item_flavour VARCHAR (50),
        item_price REAL
    );
    """
)
connection.commit()
cursor.close()

cursor = connection.cursor()
cursor.execute(
    """CREATE TABLE IF NOT EXISTS baskets (
        transaction_id INTEGER, 
        product_id INTEGER,
        FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """
)
connection.commit()
cursor.close()
connection.close()
