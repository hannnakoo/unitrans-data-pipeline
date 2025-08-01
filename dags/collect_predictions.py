# from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import requests
import psycopg2
AGENCY_ID = "unitrans"

def extract_predictions(route_id, stop_id):
    url = f"https://retro.umoiq.com/service/publicXMLFeed?command=predictions&a={AGENCY_ID}&stopId={stop_id}&routeTag={route_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()

def transform_predictions(predictions_text):
    root = ET.fromstring(predictions_text)
    prediction = root.find(".//prediction")

    if prediction is not None:
        seconds = prediction.attrib.get("seconds", 0)
        minutes = prediction.attrib.get("minutes", 0)
        print("Seconds:", seconds)
        print("Minutes:", minutes)
    else:
        print("No prediction found.")

def load_predictions():
    

p = extract_predictions("C", "181")
transform_predictions(p)