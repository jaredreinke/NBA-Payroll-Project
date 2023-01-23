from nba_api.stats.endpoints import teamyearbyyearstats
from nba_api.stats.static import teams
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time

teams_list = teams.get_teams()
teams_list = pd.DataFrame(teams_list)
id_list = teams_list["id"].to_list()

years = list(range(1990, 2024))

seasons = []
for year in years:
    start_year = str(year - 1)
    end_year = str(year)
    end_year_short = end_year[-2:]
    season = start_year + "-" + end_year_short
    seasons.append(season)

year_over_year = pd.DataFrame()
for id in id_list:
    year_by_year = teamyearbyyearstats.TeamYearByYearStats(league_id="00", team_id=id)

    df_year_by_year = pd.DataFrame(year_by_year.get_data_frames()[0])
    year_over_year = pd.concat([year_over_year, df_year_by_year])
    time.sleep(5)

# Upload to SQL Database
conn = psycopg2.connect(
    host="localhost", port=5432, database="NBA", user="postgres", password="postgres"
)

conn.autocommit = True

cur = conn.cursor()
conn.rollback()

engine = create_engine("postgresql://postgres:postgres@localhost:5432/NBA")

year_over_year.to_sql("Year Over Year", engine, if_exists="replace")

conn.commit()

conn.close()
