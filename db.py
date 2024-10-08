from dotenv import load_dotenv
import os

import mysql.connector
import json


# Load environment variables from .env file
load_dotenv()

def connect_to_db():
    return mysql.connector.connect(
        host = os.getenv('HOST'),
        user = os.getenv('USER'),
        password = os.getenv('PASSWORD'),
        database = os.getenv('DATABASE')
    )

def create_table():
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS periods (
        id INT AUTO_INCREMENT PRIMARY KEY,
        period VARCHAR(255) NOT NULL,
        incomes JSON,
        expenses JSON,
        comment TEXT
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def insert_period(period, incomes, expenses, comment):
    conn = connect_to_db()
    cursor = conn.cursor()
    # Convert incomes and expenses to JSON strings
    incomes_json = json.dumps(incomes)
    expenses_json = json.dumps(expenses)

    try:
        cursor.execute("""
            INSERT INTO periods (period, incomes, expenses, comment)
            VALUES (%s, %s, %s, %s)
        """, (period, incomes_json, expenses_json, comment))
        conn.commit()
        return {"period": period, "incomes": incomes, "expenses": expenses, "comment": comment}
    except mysql.connector.Error as err:
        conn.rollback()
        raise err
    finally:
        cursor.close()
        conn.close()
        
        
def get_all_periods():
    items = fetch_all_periods()
    periods = [item["period"] for item in items]
    return periods
        

def fetch_all_periods():
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM periods")
    res = cursor.fetchall()

    cursor.close()
    conn.close()

    # Convert JSON strings back to dictionaries
    for record in res:
        record['incomes'] = json.loads(record['incomes'])
        record['expenses'] = json.loads(record['expenses'])

    return res

def get_period(period):
    conn = connect_to_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM periods WHERE period = %s", (period,))
    res = cursor.fetchone()

    cursor.close()
    conn.close()

    if res:
        # Convert JSON strings back to dictionaries
        res['incomes'] = json.loads(res['incomes'])
        res['expenses'] = json.loads(res['expenses'])

    return res
