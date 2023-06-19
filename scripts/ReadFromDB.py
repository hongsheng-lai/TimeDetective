from mongoutils import mongo_function
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

from anomaly import AnomalyDetector
from darts.ad import KMeansScorer, QuantileDetector
from darts import TimeSeries
from darts.models import ARIMA, AutoARIMA

if __name__ == "__main__":
    uri = "mongodb+srv://user:1111@timeseries.vgcawar.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri)

    dbs = client.list_database_names()
    testdb = client.testdb
    if(testdb) in dbs:
        print("the database is existed!")
    collections_t = testdb.list_collection_names()
    
    #reset collection
    anomaly_collection_name = "anomaly_data"
    testdb[anomaly_collection_name].drop()
    #create mongo class
    mongo = mongo_function(client.testdb,"time_seriesdb_test")
    sensor_id = 1
    
    
    #the loop of reading training data (B process)
    time_interval = 20
    sensor_id = 1
    start_time = '2013-01-01 09:00:00' 
    csv_data_col = "data"
    csv_time_col='times'

    """
    User define Block
    """
    anomaly_detector = AnomalyDetector(lag=2, num_models=2)
    stage = 1
    
    while(True):
        while(mongo.findMostRecent(sensor_id,start_time,time_interval)==True):
            {}
        train_data,start_time,end_time= mongo.read_mongo(sensor_id,time_interval,csv_time_col,'',start_time)
        start_time = end_time
        if train_data.empty:
            print("data is empty!")
            break
        
        anomaly_data = train_data.copy()
        # Transform to TimeSeries
        train_data = train_data.drop_duplicates(subset=['timestamp'])
        print(train_data)
        train_data = TimeSeries.from_dataframe(train_data, 'timestamp', ['data'], fill_missing_dates=True, freq='S', fillna_value=0)
        """
        User Define Block
        """
        anomaly_detector.add_filter(train=train_data, detector=QuantileDetector(high_quantile=0.99), scorer=KMeansScorer(k=2, window=5))
        anomaly_detector.add_forecaster(train=train_data, forecaster=AutoARIMA(), detector=QuantileDetector(high_quantile=0.99), scorer=KMeansScorer(k=2, window=5))
        
        # Predict
        if stage != 1:
            binary_anom_array = anomaly_detector.detect_anomalies(train_data, type="or")
            print(binary_anom_array)

            # Write to MongoDB
            anomaly_data = anomaly_data[binary_anom_array].reset_index() # test for anomaly_data
            # print(anomaly_data)
            mongo.return_anomaly(sensor_id,anomaly_data,anomaly_collection_name,csv_data_col)
        stage += 1


        