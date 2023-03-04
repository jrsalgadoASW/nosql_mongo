import pymongo
from  datetime import datetime, timedelta

def get_semester(month):
    if month in [1, 2, 3, 4, 5, 6]:
        return "First Semester"
    elif month in [7, 8, 9, 10, 11, 12]:
        return "Second Semester"
    else:
        return None

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
    year = start_date.year
    month = start_date.month
    day = start_date.day
    hour = start_date.hour
    weekDay = start_date.weekday()

    weekDayName = start_date.strftime("%A")
    
    monthName = start_date.strftime("%B")

    dates_list.append({"DateYear":year, "DateMonth":month, "DateDay": day, "DateHour":hour, "DateWeekDay": weekDay , "DateWeekDayName": weekDayName, "DateMonthName": monthName, "Semester": get_semester(month) })
    start_date += delta

for d in dates_list:
    print(d)

# dim_date.insert_many(dates_list)



client.close()
