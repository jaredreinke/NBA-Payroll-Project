# Import neccesary modules
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# Create a list of seasons to scrape
years = list(range(1991, 2024))

seasons = []
for year in years:
    start_year = str(year - 1)
    end_year = str(year)
    season = start_year + "-" + end_year
    seasons.append(season)

# Get html text for team salary and save to salaries folder for faster loading

base_url = "https://hoopshype.com/salaries/{}"

for season in seasons:
    url = base_url.format(season)
    response = requests.get(url)
    with open("Salaries/{}".format(season), "w+") as S:
        S.write(response.text)

# Read the HTML files and create a dataframe
dfs = []
for season in seasons:
    with open("Salaries/{}".format(season)) as S:
        page = S.read()
    soup = BeautifulSoup(page, "html.parser")
    table = soup.find(
        "table",
        class_="hh-salaries-ranking-table hh-salaries-table-sortable responsive",
    )
    salaries = pd.read_html(str(table))[0]
    salaries["Season"] = season
    salaries.columns.values[0] = "Rank"
    salaries.columns.values[2] = "Yearly_Payroll"
    salaries.columns.values[-2] = "Yearly_Payroll_2023_Dollars"
    dfs.append(salaries)

salaries = pd.concat(dfs)

# Load to Postgres Database

import psycopg2

# Upload to SQL Database
conn = psycopg2.connect(
    host="localhost", port=5432, database="NBA", user="postgres", password="postgres"
)

conn.autocommit = True

cur = conn.cursor()
conn.rollback()

engine = create_engine("postgresql://postgres:postgres@localhost:5432/NBA")

salaries.to_sql("Salaries", engine, if_exists="replace")

conn.commit()

conn.close()
