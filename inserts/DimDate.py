import pymongo
from  datetime import datetime, timedelta

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Customs"]
dim_date = db["DimDate"]

start_date = datetime(2008, 1, 1, 0, 0, 0)
end_date = datetime(2028, 12, 31, 23, 0, 0)
# Create a list of dates
delta = timedelta(hours=1)
dates_list = []
while start_date <= end_date:
    dates_list.append({"date": start_date})
    start_date += delta

dim_date.insert_many(dates_list)



client.close()
