import psycopg2
import os
from dotenv import load_dotenv




def inittables():
    load_dotenv()
    conn = psycopg2.connect(
    database = "nbadb",
    user = os.environ.get("DB_USERNAME"),
    password = os.environ.get("DB_PASSWORD"),
    host = "localhost",
    port = "5432"
    )


    conn.autocommit = True

    cur = conn.cursor()

    cur.execute("""
                DROP TABLE playergamelog
                """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS players (
    player_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS playerSeasons (
            season TEXT ,
            player_id BIGINT references players(player_id),
            season_num INT,
            games_played INT,
            reb INT,
            ast INT,
            stl INT,
            blk INT,
            tov INT,
            pts INT,
            primary key (season, player_id)
    );
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS playerGamelog(
            season   TEXT    NOT NULL,
            player_id BIGINT NOT NULL,
            game_id  BIGINT  NOT NULL,
            reb INT,
            ast INT,
            stl INT,
            blk INT,
            tov INT,
            pts INT,
            PRIMARY KEY (season, player_id, game_id),
            FOREIGN KEY (player_id)
                REFERENCES players(player_id)
    );
    """)


    cur.close()
    conn.close()

