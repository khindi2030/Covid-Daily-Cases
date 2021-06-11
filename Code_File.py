import requests
import sqlite3
import json
import datetime
import pandas as pd

cmd_create_table = """ create table if not exists CASCOVID_2 (
id integer primary key autoincrement,
Daily_Active integer,
Daily_Cases integer,
Daily_Deaths integer,
Daily_Hospitalized integer,
Daily_ICU integer, 
Daily_Recovered integer, 
Daily_Tested integer, 
Date date, 
Province text, 
Total_Active integer, 
Total_Cases integer, 
Total_Deaths integer, 
Total_Hospitalized integer, 
Total_ICU integer)
"""

cmd_insert = "insert into CASCOVID_2 (Daily_Active,Daily_Cases," \
             "Daily_Deaths,Daily_Hospitalized,Daily_ICU,Daily_Recovered,Daily_Tested," \
             "Date,Province,Total_Active,Total_Cases,Total_Deaths,Total_Hospitalized,Total_ICU) " \
             "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
def request(url):

    r = requests.get(url=url)
    jsData = json.loads(r.text)
    return jsData


def insert (data):
    for tmp in data['features']:
        # print(tmp['properties'])
        Daily_Active = tmp['properties']['DailyActive']
        Daily_Cases = tmp['properties']['DailyTotals']
        Daily_Deaths = tmp['properties']['DailyDeaths']
        Daily_Hospitalized = tmp['properties']['DailyHospitalized']
        Daily_ICU = tmp['properties']['DailyICU']
        Daily_Recovered = tmp['properties']['DailyRecovered']
        Daily_Tested = tmp['properties']['DailyTested']
        Date = tmp['properties']['SummaryDate']
        Province = tmp['properties']['Province']
        Total_Active = tmp['properties']['TotalActive']
        Total_Cases = tmp['properties']['TotalCases']
        Total_Deaths = tmp['properties']['TotalDeaths']
        Total_Hospitalized = tmp['properties']['TotalHospitalized']
        Total_ICU = tmp['properties']['TotalICU']

        cur.execute(cmd_insert, [Daily_Active, Daily_Cases, Daily_Deaths, Daily_Hospitalized, Daily_ICU,
                                 Daily_Recovered,
                                 Daily_Tested, Date, Province, Total_Active, Total_Cases, Total_Deaths,
                                 Total_Hospitalized, Total_ICU])
        conn.commit()
def select():
    list=[]
    cmd_select = "select *  from CASCOVID_2 where date between ? and ?"
    sd = input("Entrer la date de debut sous format YYYY-MM-DD: ")+str('T12:00:00Z')
    dt_obj = datetime.datetime.strptime(sd,"%Y-%m-%dT%H:%M:%SZ")
    start_date = datetime.datetime.strftime(dt_obj,"%Y-%m-%dT%H:%M:%SZ")
    ed = input("Entrer la date de fin sous format YYYY-MM-DD: ")+str('T12:00:00Z')
    dte_obj = datetime.datetime.strptime(ed,"%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.datetime.strftime(dte_obj,"%Y-%m-%dT%H:%M:%SZ")
    cur.execute(cmd_select, [start_date,end_date])
    for line in cur:
        list.append(line)
    return list
def datframe(rows):
    df = pd.DataFrame(rows,
                      columns=['id', 'Daily_Active', 'Daily_Cases', 'Daily_Deaths', 'Daily_Hospitalized', 'Daily_ICU',
                               'Daily_Recovered', 'Daily_Tested', 'Date', 'Province', 'Total_Active', 'Total_Cases',
                               'Total_Deaths', 'Total_Hospitalized', 'Total_ICU'])
    return df

if __name__ == "__main__":
    url = 'https://opendata.arcgis.com/datasets/3afa9ce11b8842cb889714611e6f3076_0.geojson'
    data = request(url)
    conn = sqlite3.connect('examdb')
    cur = conn.cursor()
    cur.execute(cmd_create_table)
    insert(data)
    retrived_data = select()
    print(retrived_data)
    df = datframe(retrived_data)
    print (df.head(10))
    print(df.shape[0])
    conn.close()
