from nba_api.stats.endpoints import playercareerstats, commonallplayers, playergamelog

# Nikola Jokić
career = playercareerstats.PlayerCareerStats(player_id='203999')
players = commonallplayers.CommonAllPlayers(1)
log = playergamelog.PlayerGameLog('1630173')

# print(players.get_data_frames())
print(log.get_data_frames())
# pandas data frames (optional: pip install pandas)
# print(career.get_data_frames()[0])

# json
career.get_json()


# dictionary
career.get_dict()


