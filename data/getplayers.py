
import os
import psycopg2
import pandas as pd

from dotenv import load_dotenv
from nba_api.stats.endpoints import playercareerstats, playergamelog
from nba_api.stats.static import players
from psycopg2.extras import execute_values
from time import sleep


PLAYER_ID = 1
PLAYER_NAME = 2

playerDict = dict()

def initPlayers(conn, cur):
    df_players = pd.DataFrame(players.get_active_players())
    for row in df_players.itertuples():
        cur.execute("""
                    INSERT INTO players(player_id, full_name)
                    VALUES (%s, %s)
                    ON CONFLICT (player_id) DO NOTHING;
                    """,
                    (row[PLAYER_ID], row[PLAYER_NAME]),
                    )
        playerDict[row[PLAYER_ID]] = row[PLAYER_NAME]



def safe_player_career(player_id):
    try:
        resp = playercareerstats.PlayerCareerStats(player_id=player_id)
        dfs = resp.get_data_frames()
        return dfs
    except Exception as e:
        print(f"Skipping {player_id} {playerDict[player_id]}: {e}")
        return None


def get_player_careers(conn):
    with conn, conn.cursor() as cur:
        for pid in playerDict.keys():
            dfs = safe_player_career(pid)
            if not dfs:
                continue

            stats = dfs[0].copy()
            # 1) Prefer TOT row if it exists, otherwise keep the team row
            # Put TOT first so drop_duplicates keeps it
            stats["is_tot"] = (stats["TEAM_ABBREVIATION"] == "TOT").astype(int)
            stats = (stats.sort_values(["PLAYER_ID", "SEASON_ID", "is_tot"], ascending=[True, True, False])
                        .drop_duplicates(subset=["PLAYER_ID", "SEASON_ID"], keep="first")
                        .drop(columns=["is_tot"]))

            stats["season_start"] = stats["SEASON_ID"].str[:4].astype(int)

            # 3) Number seasons per player (rookie season = 1)
            stats = stats.sort_values(["PLAYER_ID", "season_start"])
            stats["season_num"] = stats.groupby("PLAYER_ID").cumcount() + 1

            for r in stats.itertuples(index=False):
                cur.execute(
                        """
                        INSERT INTO playerSeasons
                        (season, player_id, season_num, games_played, reb, ast, stl, blk, tov, pts)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)
                        ON CONFLICT (season, player_id) DO UPDATE SET
                        games_played = EXCLUDED.games_played,
                        reb = EXCLUDED.reb, ast = EXCLUDED.ast,
                        stl = EXCLUDED.stl, blk = EXCLUDED.blk, pts = EXCLUDED.pts;
                        """,
                        (
                        r.SEASON_ID, r.PLAYER_ID, r.season_num,
                        r.GP, r.REB, r.AST, r.STL, r.BLK, r.TOV, r.PTS
                        ),
                    )
                print(r.SEASON_ID, r.PLAYER_ID, r.season_num,
                        r.GP, r.REB, r.AST, r.STL, r.BLK, r.TOV, r.PTS)

            sleep(10)


def safe_gamelog(pid,season):
    try:
        gamelog = playergamelog.PlayerGameLog(player_id=pid, season=season).get_data_frames()
        return gamelog
    except Exception as e:
        print(f"Skipping {pid} {playerDict[pid]}: {e}")
        return None



def get_gamelog(conn, cur, season):
    for pid in playerDict.keys():
        gamelog = safe_gamelog(pid, season)
        if not gamelog:
            continue

        data = gamelog[0]
        for r in data.itertuples():
            cur.execute("""
                        INSERT INTO playerGamelog (
                        season, player_id, game_id,
                        reb, ast, stl,
                        blk, tov, pts )
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (season, player_id, game_id) DO NOTHING
                        """,
                        (r.SEASON_ID, r.Player_ID, r.Game_ID,
                         r.REB, r.AST, r.STL, r.BLK, r.TOV, r.PTS),
                        )
            print((r.SEASON_ID, r.Player_ID, r.Game_ID,
                         r.REB, r.AST, r.STL, r.BLK, r.TOV, r.PTS))
        sleep(10)




def getplayers():
    load_dotenv()

    # Connect to database
    conn = psycopg2.connect(
        database = "nbadb",
        user = os.environ.get("DB_USERNAME"),
        password = os.environ.get("DB_PASSWORD"),
        host = "localhost",
        port = "5432"
    )
    conn.autocommit = True

    #Create a cursor to perform database ops
    cur = conn.cursor()

    initPlayers(conn,cur)
    get_player_careers(conn)
    get_gamelog(conn, cur, "2024-25")


    cur.close()
    conn.close()


