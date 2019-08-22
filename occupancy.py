#!/usr/bin/python

import sys, getopt, datetime, time, math
from crate import client

def main(argv):
    crate_host = ''
    crate_user = ''
    try:
        opts, args = getopt.getopt(argv,"h:u:",["host=","user="])
    except getopt.GetoptError:
        print('occupancy.py -h <cratedb_host> -u <cratedb_user>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            crate_host = arg
        elif opt in ("-u", "--user"):
            crate_user = arg
    connection = client.connect(crate_host, username=crate_user)
    cursor = connection.cursor()
    currentTime = datetime.datetime.now().replace(microsecond=0,second=0,minute=0)
    previousDay = (currentTime - datetime.timedelta(hours=24))
    cursor.execute('SELECT "status","time_index", "entity_id", "entity_type", "fiware_servicepath" FROM "mtekz"."etparkingspot" WHERE "time_index">=? ORDER BY time_index ASC', (previousDay.strftime('%s')+'000',))
    data = cursor.fetchall()
    servicePaths = list(dict.fromkeys(map(lambda a: a[4], data)))
    occupancyData = []
    for path in servicePaths:
        pathData = filter(lambda a: a[4] == path, data)
        entityIds = list(dict.fromkeys(map(lambda a: a[2], pathData)))
        for entity in entityIds:
            cursor.execute('SELECT "status","time_index", "entity_id", "entity_type", "fiware_servicepath" FROM "mtekz"."etparkingspot" WHERE "time_index"<? AND "entity_id"=? AND "fiware_servicepath"=? ORDER BY time_index DESC', (previousDay.strftime('%s')+'000', entity, path))
            previousState = cursor.fetchone()
            if previousState:
                previousState = previousState[0]
            else:
                previousState = 'free'
            entityData = filter(lambda a: a[2] == entity, pathData)
            if entityData and len(entityData) > 0:
                entity_type = entityData[0][3]
            else:
                entity_type = None
            for i in range(24):
                start_time = (currentTime - datetime.timedelta(hours=(24-i))).strftime('%s')+'000'
                end_time = (currentTime - datetime.timedelta(hours=(24-i-1))).strftime('%s')+'000'
                hourData = filter(lambda d: d[1]>int(start_time) and d[1]<int(end_time), entityData)
                occupiedTime = 0
                for j in range((len(hourData))):
                    timePassed = 0
                    if j == 0:
                        timePassed = hourData[j][1] - int(start_time)
                    elif j == (len(hourData)-1):
                        timePassed = int(end_time) - hourData[j][1]
                    else:
                        timePassed = hourData[j+1][1] - hourData[j][1]
                    if previousState == 'occupied':
                        occupiedTime = occupiedTime + timePassed
                    if j != (len(hourData)-1):
                        previousState = hourData[j+1][0]
                    if not previousState:
                        previousState = 'free'
                occupancy = int(math.ceil((occupiedTime/3600000.0)*100))
                occupancyData.append((occupancy, start_time, entity, entity_type, path))
    cursor.executemany('INSERT INTO "mtekz"."etparkingoccupancy" (occupancy, time_index, entity_id, entity_type, fiware_servicepath) VALUES (?,?,?,?,?)', occupancyData)
    sys.exit()

if __name__ == "__main__":
   main(sys.argv[1:])