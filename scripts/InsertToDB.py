from mongoutils import mongo_function
from pymongo import MongoClient


if __name__ == "__main__":
    uri = "mongodb+srv://user:1111@timeseries.vgcawar.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri)

    dbs = client.list_database_names()
    testdb = client.testdb
    if(testdb) in dbs:
        print("the database is existed!")
    collections_t = testdb.list_collection_names()


    #reset collection
    collection_name = "time_seriesdb_test"
    testdb[collection_name].drop()
    #create mongo class
    mongo = mongo_function(client.testdb,collection_name)
    sensor_id = 1
    #insert sener data to mongodb (A process)
    csv_cols =['data','times']

    mongo.insert_mongo(sensor_id,'./Sensor1.csv',csv_cols)