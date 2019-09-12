#!/usr/bin/python

import sys, getopt, datetime, pytz, time, math
from dateutil import parser
from tzlocal import get_localzone
from crate import client

def main(argv):
    # Setting up the connection to CrateDB (with command line args)
    crate_host = ''
    crate_user = ''
    start_date = None
    end_date = None
    try:
        opts, args = getopt.getopt(argv,"h:u:s:e:",["host=","user=", "start-date=", "end-date="])
    except getopt.GetoptError:
        print('occupancy.py -h <cratedb_host> -u <cratedb_user>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            crate_host = arg
        elif opt in ("-u", "--user"):
            crate_user = arg
        elif opt in ("-s", "--start-date"):
            start_date = parser.parse(arg)
        elif opt in ("-e", "--end-date"):
            end_date = parser.parse(arg)
    connection = client.connect(crate_host, username=crate_user)
    cursor = connection.cursor()
    comoputeOccupancy(cursor, start_date, end_date)

def comoputeOccupancy(cursor, start_date, end_date):
    # Current time changed if end date specified
    if end_date:
        currentTime = end_date.replace(microsecond=0,second=0,minute=0,hour=0,tzinfo=pytz.UTC)
    else:
        currentTime = datetime.datetime.now().replace(microsecond=0,second=0,minute=0,hour=0,tzinfo=pytz.UTC)

    # Start time changed if given a start date
    if start_date:
        previousTime = start_date.replace(microsecond=0,second=0,minute=0,hour=0,tzinfo=pytz.UTC)
    else:
        previousTime = (currentTime - datetime.timedelta(hours=24))

    # How many hours to compute data for
    hoursDiff = int((currentTime-previousTime).total_seconds()/60/60)

    # Check if hours not 0 or negative
    if hoursDiff < 1:
        print("Start date too close to current time")
        sys.exit(2)

    # Setting up cursor and pulling data since the start time
    cursor.execute('SELECT "status","time_index", "entity_id", "entity_type", "fiware_servicepath", "name", "refdevice" FROM "mtekz"."etparkingspot" WHERE "time_index">=? AND "time_index"<=? AND status!=? AND status!=? ORDER BY time_index ASC LIMIT 1000000', (previousTime.strftime('%s')+'000', currentTime.strftime('%s')+'000', 'None', 'unknown'))
    data = cursor.fetchall()

    # List of Service Paths
    servicePaths = list(dict.fromkeys(map(lambda a: a[4], data)))

    # Computing occupancy data for all entities and servicepaths
    occupancyData = []
    for path in servicePaths:
        pathData = filter(lambda a: a[4] == path, data)
        entityIds = list(dict.fromkeys(map(lambda a: a[2], pathData)))
        for entity in entityIds:
            # Getting the last known status. Assumed free if none found
            cursor.execute('SELECT "status","time_index", "entity_id", "entity_type", "fiware_servicepath", "name", "refdevice" FROM "mtekz"."etparkingspot" WHERE "time_index"<? AND "entity_id"=? AND "fiware_servicepath"=? AND status!=? AND status!=? ORDER BY time_index DESC', (previousTime.strftime('%s')+'000', entity, path, 'None', 'unknown'))
            previousStateRow = cursor.fetchone()
            if previousStateRow:
                previousState = previousStateRow[0]
            else:
                previousState = 'free'
            entityData = filter(lambda a: a[2] == entity, pathData)
            # Setting these to None as defaults
            entity_type = None
            name = None
            refdevice = None
            # For each of the hours since the start time given
            for i in range(hoursDiff):
                start_time = (currentTime - datetime.timedelta(hours=(hoursDiff-i+1))).strftime('%s')+'000'
                end_time = (currentTime - datetime.timedelta(hours=(hoursDiff-i))).strftime('%s')+'000'
                hourData = filter(lambda d: d[1]>=int(start_time) and d[1]<int(end_time), entityData)
                occupiedTime = 0
                if len(hourData) > 0:
                    for j in range((len(hourData)+1)):
                        if j != len(hourData):
                            if hourData[j][3]:
                                entity_type = hourData[j][3]
                            if hourData[j][5]:
                                name = hourData[j][5]
                            if hourData[j][6]:
                                refdevice = hourData[j][6]
                        timePassed = 0
                        if j == 0:
                            timePassed = hourData[j][1] - long(start_time)
                        elif j == len(hourData):
                            timePassed = long(end_time) - hourData[j-1][1]
                        else:
                            timePassed = hourData[j][1] - hourData[j-1][1]
                        if previousState == 'occupied':
                            occupiedTime = occupiedTime + timePassed
                        if j != len(hourData) and hourData[j][0]:
                            previousState = hourData[j][0]
                if len(hourData) == 0 and previousState == 'occupied':
                    occupiedTime = 3600000
                if len(hourData) == 0 and previousState == 'free':
                    occupiedTime = 0
                occupancy = int(math.ceil((occupiedTime/3600000.0)*100))
                timezonedStartTime = datetime.datetime.fromtimestamp(long(start_time)/1000.0).strftime('%s')+'000'
                occupancyData.append((occupancy, timezonedStartTime, entity, entity_type, path, name, refdevice))
    cursor.executemany('INSERT INTO "mtekz"."etparkingoccupancy" (occupancy, time_index, entity_id, entity_type, fiware_servicepath, name, refdevice) VALUES (?,?,?,?,?,?,?)', occupancyData)
    sys.exit()

if __name__ == "__main__":
   main(sys.argv[1:])
