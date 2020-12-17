import requests 
import json
import matplotlib.pyplot as plt
import pandas as pd
import json
import pandas as pd
import datetime

#provided starter code by Gina Sprint
#Added onto for project and also is cleaned data
def merge_data (filename): 
    infile = open(filename, "r")
    json_object = json.load(infile)

    timeline_array = json_object["timelineObjects"]

    activity_df = pd.DataFrame(dtype=object)
    place_df = pd.DataFrame(dtype=object)
    for timeline_object in timeline_array:
        ser = pd.Series(dtype=object)
        if "activitySegment" in timeline_object.keys():    
            activity_segment = timeline_object["activitySegment"]
            ser["activityType"] = activity_segment["activityType"]
       
            duration = activity_segment["duration"]
            ser["Dates"] = duration["startTimestampMs"]
       
            ser["distance(m)"] = activity_segment["distance"]
        
            ser["confidence"]= activity_segment["confidence"]
        
            activities = activity_segment["activities"]
            zero = activities[0]
            ser["probability"] = zero["probability"]
        
            activity_df = activity_df.append(ser, ignore_index=True)
        
        elif "placeVisit" in timeline_object.keys():
            place_visit = timeline_object["placeVisit"]
            location = place_visit["location"]
            ser["name"] = location["name"]
            ser["address"] = location["address"]
            ser["locationConfidence"] = location["locationConfidence"]
    
       
            duration = place_visit["duration"]
            ser["Dates"] = duration["startTimestampMs"]
        
            ser["placeConfidence"] = place_visit["placeConfidence"]
       
            place_df = place_df.append(ser, ignore_index=True)
        else:
            print("unknown timeline object type")
        
    activity_df["Dates"] = pd.to_datetime(activity_df["Dates"], unit='ms')
    activity_df["Dates"] = pd.to_datetime(activity_df["Dates"]).dt.date

    place_df["Dates"] = pd.to_datetime(place_df["Dates"], unit='ms')
    place_df["Dates"] = pd.to_datetime(place_df["Dates"]).dt.date

    activity_df = activity_df.reindex(columns=["Dates", "activityType","distance(m)","confidence","probability"])
    place_df = place_df.reindex(columns=["Dates","name","address","placeConfidence","locationConfidence"])

    merged_df =  activity_df.merge(place_df, on="Dates")
    return merged_df

# Next 2 functions provided by MeteoStat Lecture and access weather API to create Temp(F) column  
def vancouver_weather(start_date, end_date, df):
    api_key = "FmN2dXW49ZQEJepnmN1JAZdxmmjeVvHd"
 
    headers= {"x-api-key": api_key}
    url = "https://api.meteostat.net/v2/stations/search"
    url+= "?query=portland"

    response = requests.get(url=url, headers=headers)
    json_object = json.loads(response.text)

    data_object = json_object["data"][0]
    vancouver_id = "72698"

    url="https://api.meteostat.net/v2/stations/daily"
    url+= "?station=" + vancouver_id
    url+= "&start=" + start_date
    url+= "&end=" + end_date

    response = requests.get(url=url, headers=headers)
    json_object = json.loads(response.text)
    data_object = json_object["data"]
    tmax_ser = pd.Series(dtype=float)
    for date_object in data_object:
        date=date_object["date"]
        tmax=date_object["tmax"]*(9/5)+32
        tmax_ser[date] = tmax

    tmax_ser.to_csv("WEATHER.csv")
    data = pd.read_csv("WEATHER.csv")
    data.rename(columns={'Unnamed: 0':'Dates'}, inplace=True)
    data.rename(columns={'0':'Temp(F)'}, inplace=True)
    weather_df = pd.DataFrame(data)

    df["Dates"] = df["Dates"].astype(str)

    merge_df = df.merge(weather_df, on="Dates")
    return merge_df

def spokane_weather(start_date, end_date, df):
    api_key = "FmN2dXW49ZQEJepnmN1JAZdxmmjeVvHd"

    headers= {"x-api-key": api_key}
    url = "https://api.meteostat.net/v2/stations/search"
    url+= "?query=spokane"

    response = requests.get(url=url, headers=headers)
    json_object = json.loads(response.text)
  

    data_object = json_object["data"][0]
    spokane_id = data_object["id"]

    url="https://api.meteostat.net/v2/stations/daily"
    url+= "?station=" + spokane_id
    url+= "&start=" + start_date
    url+= "&end=" + end_date

    response = requests.get(url=url, headers=headers)
    json_object = json.loads(response.text)
    data_object = json_object["data"]
    tmax_ser = pd.Series(dtype=float)
    for date_object in data_object:
        date=date_object["date"]
        tmax=date_object["tmax"]*(9/5)+32
        tmax_ser[date] = tmax
    
    tmax_ser.to_csv("WEATHER.csv")
    data = pd.read_csv("WEATHER.csv")
    data.rename(columns={'Unnamed: 0':'Dates'}, inplace=True)
    data.rename(columns={'0':'Temp(F)'}, inplace=True)
    weather_df = pd.DataFrame(data)

    df["Dates"] = df["Dates"].astype(str)

    merge_df = df.merge(weather_df, on="Dates")
    return merge_df

#adds days of the week column 
def add_weekday(df):
    df["Dates"] = pd.to_datetime(df['Dates'])   
    df['Day of week'] = df['Dates'].dt.day_name()
    return df
