
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

MASTER_PASSWORD = os.getenv("DB_PASSWORD")
ENDPOINT = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
USER = os.getenv("DB_USER")
DBNAME = os.getenv("DB_NAME")
REGION=os.getenv("AWS_DEFAULT_REGION")


try:
    conn = psycopg2.connect(
    host=ENDPOINT,
    port=PORT,
    database=DBNAME,
    user=USER,
    password=MASTER_PASSWORD
)
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))                
                