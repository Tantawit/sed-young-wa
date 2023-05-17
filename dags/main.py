from datetime import datetime, timedelta
from os.path import dirname, abspath
import os
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import requests
import pandas as pd
from tqdm import tqdm
import pytz
import time as os_time
import random 
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
import dateparser
from pyspark.sql import SparkSession
from datetime import datetime
import json 

pd.options.mode.chained_assignment = None
# sys.path.append('./opt/airflow/data/text')

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'sedYoungWa',
    'retries': 5,
    'retry_delay':timedelta(minutes=5)
}

# <=== Config ===>
# careful try/except at `load traffy` and  `create weather` part 
DATA_DIR = '/opt/airflow/data'
RESULT_DIR = '/opt/airflow/result'

# training config
# number of day for scraping weather 
DAY_OF_HIST_WEATHER = 40 

# the year of holiday data.
YEAR_IN_THAI = 2566

# using data for training between [0, current_day - TIME_OFFSET]
TIME_OFFSET = 4 

def get_api_key():
  url = requests.get("https://www.wunderground.com/weather/th/bangkok/VTBD")
  soup = BeautifulSoup(url.content, 'html.parser')

  soup = soup.find("script", {"id":"app-root-state"}).text

  start_idx = soup.find("apiKey=")
  end_idx = soup.find("&a", start_idx)

  api_key = soup[start_idx: end_idx].replace("apiKey=", "")

  return api_key

def fill_missing(df):
  if len(df) != 48:
    periods = ['00:00', '00:30', '01:00', '01:30', '02:00', '02:30',
                '03:00', '03:30', '04:00', '04:30', '05:00', '05:30',
                '06:00', '06:30', '07:00', '07:30', '08:00', '08:30',
                '09:00', '09:30', '10:00', '10:30', '11:00', '11:30',
                '12:00', '12:30', '13:00', '13:30', '14:00', '14:30',
                '15:00', '15:30', '16:00', '16:30', '17:00', '17:30',
                '18:00', '18:30', '19:00', '19:30', '20:00', '20:30',
                '21:00', '21:30', '22:00', '22:30', '23:00', '23:30']
    for period in periods:
        if period not in df['time'].values:
            df = df.append({'time': period}, ignore_index=True)
    df = df.sort_values(by=['time'])
    df = df.fillna(method='ffill')
    df = df.drop_duplicates(subset=['time'], keep='first')
    df = df.reset_index(drop=True)
  return df

def scrap_weather_v2(n_day):

  api_key = get_api_key()

  all_df = pd.DataFrame()

  today = datetime.now()
  for i in tqdm(range(1, n_day + 1)):
    df = pd.DataFrame(columns=["date","time","temp (F)","feel_like (F)","dew_point (F)","humidity (%)","wind","wind_speed (mph)","wind_gust (mph)","pressure (in)","precip (in)","condition"])
    target_date = today - timedelta(days=i)
    historical_data = requests.get("https://api.weather.com/v1/location/VTBD:9:TH/observations/historical.json",
                                  params={
                                      "apiKey": api_key,
                                      "units": "e",
                                      "startDate": target_date.strftime("%Y%m%d")
                                  }
                                  ).json()                
    historical_data['observations']
    for data in historical_data['observations']:
      date_time = datetime.fromtimestamp(data['valid_time_gmt'], pytz.timezone("Asia/Bangkok"))
      date = date_time.strftime('%Y-%m-%d')
      time = date_time.strftime('%H:%M')
      new_row = {
          "date": date,
          "time": time,
          "temp (F)": data["temp"],
          "feel_like (F)": data["feels_like"],
          "dew_point (F)": data["dewPt"],
          "humidity (%)": data["rh"],
          "wind": data["wdir_cardinal"],
          "wind_speed (mph)": data["wspd"] or 0,
          "wind_gust (mph)": data["gust"] or 0,
          "pressure (in)": data["pressure"],
          "precip (in)": data["precip_hrly"] or 0,
          "condition": data["wx_phrase"],
      }

      df = df.append(new_row, ignore_index=True)

    df = df.dropna()
    df = fill_missing(df)

    all_df = all_df.append(df, ignore_index=True)

    os_time.sleep(random.randint(1, 3))

  return all_df

def concat_datetime(data) :
  date = data['date'] + " " + data['time']
  return datetime.strptime(date, "%Y-%m-%d %H:%M")

def scape_data():
    # Download file from traffy fondue
    import urllib.request
    urllib.request.urlretrieve("https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv", filename="/opt/airflow/data/bangkok_traffy.csv")

    # Initial Spark
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    
    df_weather = scrap_weather_v2(n_day=DAY_OF_HIST_WEATHER)
    df_weather['datetime'] = df_weather.apply(concat_datetime, axis=1)
    df_weather.head()
    df_weather.to_csv(os.path.join(DATA_DIR, "weather_data"))

    # Get Holiday
    url = f"https://calendar.kapook.com/{YEAR_IN_THAI}/holiday" 

    url = requests.get(url)
    soup = BeautifulSoup(url.content, 'html.parser')

    soup = soup.find('div', {"id": "holiday_wrap"}).find_all('span', {"class": "date"})
    holidays = set()
    for x in soup :
        x = x.text
        dt = dateparser.parse(x)
        dt = datetime(dt.year - 543, dt.month, dt.day)
        holidays.add(dt.strftime("%d/%m/%Y"))

    thai_holidays = {"days": list(holidays)}

    with open(os.path.join(DATA_DIR,"thai_holidays.json"), "w") as outfile:
        json_object = json.dumps(thai_holidays, indent = 4, ensure_ascii=False) 
        print(json_object)
        outfile.write(json_object)

    path = os.path.join(DATA_DIR, "bangkok_traffy.csv")

    spark_df = spark.read.csv(path, header=True, inferSchema=True, sep=',', escape="\"", encoding='utf-8', multiLine=True)

    df_raw = spark_df.toPandas()
    df_raw.tail(5).to_csv(os.path.join(DATA_DIR, "sample_query.csv"))
    df_raw.head(2)
   

def train_model():
    print("Train!")

def deploy_model():
    print("Deploy!")

with DAG(
    default_args=default_args,
    dag_id='test3',
    description="Pipeline for training and deploying Sed Yound Wa model",
    schedule_interval='@daily',
    start_date=datetime(2023, 5, 13),
) as dag:
    # import mlflow
    # mlflow.set_tracking_uri("http://localhost:5000")
    # mlflow.set_experiment("Toxic Comment Classifier")
    # mlflow.sklearn.autolog(silent=True, log_models=False)
    scape_task = PythonOperator(
        task_id='scape',
        python_callable=scape_data
    )
    train_task = PythonOperator(
        task_id='train',
        python_callable=train_model
    )
    deploy_task = PythonOperator(
        task_id='deploy',
        python_callable=deploy_model
    )

    scape_task >> train_task >> deploy_task