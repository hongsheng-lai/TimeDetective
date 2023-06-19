from datetime import datetime, timedelta
import pandas as pd
import sys
import os
from pymongo import MongoClient
import time

class mongo_function():
    def __init__(self,database,collection):
        self.db = database
        self.collec = self.db[collection]
        collections_total = self.db.list_collection_names()
        if collection in collections_total:
            print(f"{collection} collection is exist!")
        else:
            self.db.create_collection(
                collection,
                timeseries = {
                    "timeField": "timestamp",
                    "granularity": "seconds"
                }
            )
            print(f"{self.collec} collection is create!")
    '''
    usage:
        insert each row of data from csv to mongodb every second
    parameters:
        id = id of sensor , (int)
        csv_path = the data of sensor , (string)
        csv_col = all the header name of the csv in sequential , (list)
    '''
    def insert_mongo(self,id,csv_path,csv_col):
        #csv_path = './Sensor1.csv'
        sensor = pd.read_csv(csv_path)
        sensor = pd.DataFrame(sensor,columns=csv_col)
        for i in range(0,sensor.shape[0]):
            data = {
                    "sensorID": id,
                    csv_col[0]: sensor[csv_col[0]][i],
                    "timestamp": datetime.strptime(sensor[csv_col[1]][i],"%Y-%m-%d %H:%M:%S")
                }
            inserted_id = self.collec.insert_one(data).inserted_id
            time.sleep(1)
    '''
    usage:
        download a set of data from mongodb with user defined time interval
    parameters:
        id = id of sensor , (int)
        time_interval = how long does the time last , (int)
        csv_path = the data of sensor , (string)
        time_col_name = the header name of the timestamp column

        start_time = when is the first data start in this anomaly detection , (string)
            start_time='' => start time set as the csv first row's time
            start time='2013-01-01 09:00:00' => user define time

        return:
            pd.DataFrame(iter(total)) : train data
            str(end_time) : next start_time
    '''
    def read_mongo(self,id,time_interval,time_col_name,csv_path="",start_time=""):
        if(csv_path!=""):
            initial_time = (pd.DataFrame(pd.read_csv(csv_path,nrows=1)[time_col_name])[time_col_name][0])
        if(start_time ==""):
            start_time = datetime.strptime(initial_time, "%Y-%m-%d %H:%M:%S")
        else:
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        
        end_time = start_time+timedelta(seconds=time_interval)
        total = self.collec.find({"sensorID":id,"timestamp": {
        '$gte':start_time,
        '$lt': end_time}
        },
        {'_id':0})
        return pd.DataFrame(iter(total)),str(start_time),str(end_time)
    '''
    usage:
        insert anomaly data back to mongodb
    parameters:
        id = id of sensor , (int)
        anomaly_data = anomaly_data , (pandas.dataframe)
        anomaly_collection = collection which you want to store anomaly data, (string)

    '''
    def return_anomaly(self,id,anomaly_data,anomaly_collection,data_col):
        collections_total = self.db.list_collection_names()
        if anomaly_collection not in collections_total:
            self.db.create_collection(
                anomaly_collection,
                timeseries = {
                    "timeField": "timestamp",
                    "granularity": "seconds"
                }
            )
        for i in range(0,anomaly_data.shape[0]):
            data = {
                    "sensorID": id,
                    data_col: anomaly_data[data_col][i],
                    "timestamp": anomaly_data['timestamp'][i]
                }
            inserted_id = self.db[anomaly_collection].insert_one(data).inserted_id

    def findMostRecent(self,id,start_time,time_interval):
        total = pd.DataFrame(iter(self.collec.find({"sensorID":id}).sort("timestamp",-1).limit(1)))
        Recenttime = total['timestamp'][0] + timedelta(seconds=1)

        next_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=time_interval)
        if(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")>=Recenttime):
            return False
        return Recenttime<next_time