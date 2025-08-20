import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
from main import initdb


def consistency():
    conn = initdb()
    cur = conn.cursor()


    cur.execute("SELECT * FROM playerSeasons")

    rows = cur.fetchall()   # fetch all rows
    print(rows[0])

