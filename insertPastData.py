import sqlite3
import csv
import datetime

# variables needed for importing
dbName = "home-assistant_v2.db"
entityName = "sensor.kozolec_total_pv_energy"
importFile = "projects/kozolecTotalProduction.csv"
unitDivision = 1
resetInterval = "" # H, D, M... 
dateFormat = '%Y-%m-%dT%H:%M:%SZ'
statisticsMaxDeviation = 3

conn = sqlite3.connect(dbName)
c= conn.cursor()
states_meta_id = c.execute("SELECT * FROM states_meta where entity_id = '" + entityName + "'").fetchmany(1)[0][0]
print("states id: {}".format(states_meta_id))
statistics_meta_id = c.execute("SELECT * FROM statistics_meta where statistic_id = '" + entityName + "'").fetchmany(1)[0][0]
print("statistics id: {}".format(statistics_meta_id))

if not states_meta_id:
    print("cannot find states for given entity")
if not statistics_meta_id:
    print("cannot find statistics for given entity")

# read and parse data
file = open(importFile)
fileContents = csv.reader(file,  dialect = 'excel', delimiter=",", quotechar="\"")
if len(dateFormat) > 0:
    data = [(datetime.datetime.strptime(a, dateFormat), float(b)) for (a, b) in fileContents]
else:
    data = [(datetime.datetime.fromtimestamp(int(a)), float(b)) for (a, b) in fileContents]

timeStamp = datetime.datetime.fromtimestamp(0)
lastHour = -1
value=0
statisticsValue=0

startingTimestamp = data[0][0]
endingTimestamp = data[-1][0]
print(startingTimestamp, startingTimestamp.timestamp())
print(endingTimestamp, endingTimestamp.timestamp())

# clear any data for the time range in the import file
c.execute("DELETE FROM states where metadata_id = "+ str(states_meta_id) + " and last_updated_ts between " + str(startingTimestamp.timestamp()) + " and " + str(endingTimestamp.timestamp()-1))
c.execute("DELETE FROM statistics where metadata_id = "+ str(statistics_meta_id) + " and start_ts between " + str(startingTimestamp.timestamp()) + " and " + str(endingTimestamp.timestamp()-1))
c.execute("DELETE FROM statistics_short_term where metadata_id = "+ str(statistics_meta_id) + " and start_ts between " + str(startingTimestamp.timestamp()) + " and " + str(endingTimestamp.timestamp()-1))

# retrieve entries before and after imported tada
lastPrevoiusRow = c.execute("SELECT * FROM states where metadata_id = " + str(states_meta_id) + " and last_updated_ts = (select max(last_updated_ts) from states where metadata_id = " + str(states_meta_id) + " and last_updated_ts <= " + str(startingTimestamp.timestamp()) + " )").fetchmany(1)
firstNextRow = c.execute("SELECT * FROM states where metadata_id = " + str(states_meta_id) + " and last_updated_ts = (select min(last_updated_ts) from states where metadata_id = " + str(states_meta_id) + " and last_updated_ts >= " + str(endingTimestamp.timestamp()) + " )").fetchmany(1)
if len(lastPrevoiusRow) > 0:
    oldStateId = lastPrevoiusRow[0][0]
else:
    oldStateId = 'null'
# print(lastPrevoiusRow)
# print(firstNextRow)
previousTimeStamp = startingTimestamp
previousInterval = startingTimestamp
sumValue = 0
statisticsSumValue = 0

# retrieve last statiscic to offset sums
lastStat = c.execute("SELECT state, sum FROM statistics where metadata_id = " + str(statistics_meta_id) + " and created_ts = (select max(created_ts) from statistics where metadata_id = " + str(statistics_meta_id) + " and created_ts <= " + str(startingTimestamp.timestamp()) + " )").fetchmany(1)
if len(lastStat) > 0:
    sumValue = float(lastStat[0][0])
    statisticsSumValue = float(lastStat[0][1])

for row in data:
    # sum values for a month
    if row[0] and row[1]:
        timeStamp = row[0]
        value = row[1]/unitDivision
        sumValue = sumValue + value
        statisticsSumValue = statisticsSumValue + value
        if(lastHour >= 0 and lastHour != timeStamp.hour and timeStamp.timestamp() != previousTimeStamp.timestamp()):
            # print(timeStamp, previousTimeStamp)
            # print("INSERT into statistics (created_ts, metadata_id, start_ts, state, sum) values ({}, {}, {}, {}, {} )".format(timeStamp.timestamp(), statistics_meta_id, previousTimeStamp.timestamp(), sumValue, statisticsSumValue))
            c.execute("INSERT into statistics (created_ts, metadata_id, start_ts, state, sum) values ({}, {}, {}, {}, {} )".format(timeStamp.timestamp(), statistics_meta_id, previousTimeStamp.timestamp(), sumValue, statisticsSumValue))
            previousTimeStamp = timeStamp
        lastHour = timeStamp.hour
        oldStateId = c.execute("INSERT into states (metadata_id, state, last_updated_ts, origin_idx, old_state_id) values ({}, {}, {}, {}, {} ) returning state_id".format(states_meta_id, sumValue, timeStamp.timestamp(), 0, oldStateId)).lastrowid
        if resetInterval == "D"  and previousInterval.day != timeStamp.day:
            sumValue = 0
            previousInterval = timeStamp

        # print(timeStamp)
        # print(value)
print(sumValue)
c.fetchall()
print(firstNextRow[0][0])
c.execute("UPDATE states set old_state_id = " + str(oldStateId) + " where state_id = " + str(firstNextRow[0][0]))

# update sum for later statistics
c2 = conn.cursor()
prevState = sumValue

lastStatSumValue = statisticsSumValue
for row in c.execute("SELECT id,state, sum FROM statistics where metadata_id = " + str(statistics_meta_id) + " and created_ts >= " + str(endingTimestamp.timestamp()) + " order by created_ts asc").fetchall():
    # print(str(row[1]) + "   -   " + str(prevState))
    difference  = float(row[1])  if float(row[1]) - prevState < - statisticsMaxDeviation else float(row[1]) - prevState 
    prevState = float(row[1])
    # print("correcting sumOffset: {} difference {} ".format(statisticsSumValue, difference))
    statisticsSumValue = statisticsSumValue + difference
    c2.execute("UPDATE statistics SET sum = " + str(statisticsSumValue) + " where id = " + str(row[0]) +"")

prevState = sumValue
statisticsSumValue = lastStatSumValue
for row in c.execute("SELECT id,state, sum FROM statistics_short_term where metadata_id = " + str(statistics_meta_id) + " and created_ts >= " + str(endingTimestamp.timestamp()) + " order by created_ts asc").fetchall():
    # print(str(row[1]) + "   -   " + str(prevState))
    difference  = float(row[1])  if float(row[1]) - prevState < - statisticsMaxDeviation else float(row[1]) - prevState 
    prevState = float(row[1])
    # print("correcting sumOffset: {} difference {} ".format(statisticsSumValue, difference))
    statisticsSumValue = statisticsSumValue + difference
    c2.execute("UPDATE statistics_short_term SET sum = " + str(statisticsSumValue) + " where id = " + str(row[0]) +"")

conn.commit()
conn.close()