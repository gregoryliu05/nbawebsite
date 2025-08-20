from inittables import *
from getplayers import *
from consistency import *
import os
from dotenv import load_dotenv


def initdb():
    load_dotenv()
    conn = psycopg2.connect(
    database = "nbadb",
    user = os.environ.get("DB_USERNAME"),
    password = os.environ.get("DB_PASSWORD"),
    host = "localhost",
    port = "5432"
    )
    return conn



def main():
    # inittables()
    # getplayers()
    consistency()



if __name__ == "__main__":
    main()
