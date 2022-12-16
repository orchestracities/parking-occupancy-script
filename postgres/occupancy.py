#!/usr/bin/python

import datetime
import getopt
import logging
import math
import pytz
import sys
import psycopg2
from dateutil import parser

def main(argv):
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Setting up the connection to CrateDB (with command line args)
    pg_host = None
    pg_user = None
    pg_password = None
    start_date = None
    end_date = None
    delta = 24
    tenant_name = 'dietikon'
    cursor = None
    connection = None
    dry_run = False
    try:
        opts, args = getopt.getopt(argv, "h:u:p:t:s:e:d:r:",
                                   ["host=", "user=", "password=",
                                    "tenant-name=", "start-date=",
                                    "end-date=", "delta-time", "dry-run"])
    except getopt.GetoptError:
        logger.error("wrong parameters")
        print('occupancy.py -h <db_host> -u <db_user>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            pg_host = arg
        elif opt in ("-u", "--user"):
            pg_user = arg
        elif opt in ("-p", "--password"):
            pg_password = arg
        elif opt in ("-t", "--tenant-name"):
            tenant_name = arg
        elif opt in ("-s", "--start-date"):
            start_date = parser.parse(arg)
        elif opt in ("-e", "--end-date"):
            end_date = parser.parse(arg)
        elif opt in ("-d", "--delta-time"):
            delta = int(arg)
        elif opt in ("-r", "--dry-run"):
            dry_run = bool(arg)
    if not pg_host:
        logger.error("missing parameters")
        print('occupancy.py -h <cratedb_host> -u <cratedb_user>')
        sys.exit(-2)
    try:
        logger.info("connecting...")
        schema = "mt" + tenant_name.lower()
        connection = psycopg2.connect(database="quantumleap",
                        host=pg_host,
                        user=pg_user,
                        password=pg_password,
                        port=5432)
        cursor = connection.cursor()
        computeOccupancy(cursor, schema, start_date, end_date, delta, dry_run)
        connection.commit()
    except Exception as e:
        logger.error(str(e), exc_info=True)
        sys.exit(-2)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        sys.exit()


def computeOccupancy(cursor, schema, start_date, end_date, delta, dry_run):
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("computing occupancy...")
    # Current time changed if end date specified
    if end_date:
        currentTime = end_date.replace(microsecond=0, second=0, minute=0,
                                       tzinfo=pytz.UTC)
    else:
        currentTime = datetime.datetime.utcnow().replace(microsecond=0, second=0,
                                                      minute=0,
                                                      tzinfo=pytz.UTC)

    # Start time changed if given a start date
    if start_date:
        previousTime = start_date.replace(microsecond=0, second=0, minute=0,
                                          tzinfo=pytz.UTC)
    else:
        previousTime = (currentTime - datetime.timedelta(hours=delta))

    # How many hours to compute data for
    hoursDiff = int((currentTime - previousTime).total_seconds() / 60 / 60)

    # Check if hours not 0 or negative
    if hoursDiff < 1:
        logger.error("Start date too close to current time")
        print("Start date too close to current time")
        sys.exit(-2)

    # Setting up cursor and pulling data since the start time
    limit = 1000
    offset = 0
    data = []
    query = "SELECT status, time_index, entity_id, entity_type, " \
           "fiware_servicepath, name, refdevice FROM " \
           "{}.etparkingspot WHERE time_index>='{}' AND time_index<='{}' " \
           "AND status!='None' AND status!='unknown' ORDER BY time_index ASC LIMIT {} " \
           "OFFSET {}"
    while offset >= 0:
        stmt = query.format(schema, previousTime.isoformat(), currentTime.isoformat(), limit, offset)
        print(stmt)
        cursor.execute(stmt)
        current_size = len(data)
        data += cursor.fetchall()
        if len(data) == current_size:
            offset = -1
        else:
            offset += limit
    logger.info("loaded {} data".format(len(data)))

    # List of Service Paths
    servicePaths = list(dict.fromkeys(map(lambda a: a[4], data)))

    # Computing occupancy data for all entities and servicepaths
    occupancyData = []
    for path in servicePaths:
        pathData = list(filter(lambda a: a[4] == path, data))
        entityIds = list(dict.fromkeys(map(lambda a: a[2], pathData)))
        for entity in entityIds:
            # Getting the last known status. Assumed free if none found
            stmt = "SELECT status, time_index, entity_id, " \
                   "entity_type, fiware_servicepath, name, refdevice " \
                   "FROM {}.etparkingspot WHERE time_index<'{}' AND " \
                   "entity_id='{}' AND fiware_servicepath='{}' AND status!='None' " \
                   "AND status!='unknown' ORDER BY time_index DESC LIMIT 1".format(schema, previousTime, entity, path)
            cursor.execute(stmt)
            previousStateRow = cursor.fetchone()
            if previousStateRow:
                previousState = previousStateRow[0]
                entity_type = previousStateRow[3]
                name = previousStateRow[5]
                refdevice = previousStateRow[6]
            else:
                previousState = 'free'
                entity_type = None
                name = None
                refdevice = None
            entityData = list(filter(lambda a: a[2] == entity, pathData))
            occupancyData.extend(computeEntityOccupancy(entity, entity_type, name, path, refdevice, entityData, previousState, previousTime, hoursDiff))

    logger.info("occupancy computed")
    if dry_run:
        logger.info("dry run mode, no data will be stored")
    stmt = "INSERT INTO {}.etparkingoccupancy (occupancy, time_index, entity_id, entity_type, fiware_servicepath, name, refdevice) VALUES (%s,%s,%s,%s,%s,%s,%s)".format(schema)
    for i in range(0, len(occupancyData), 1000):
        chunck = occupancyData[i:i + 1000]
        if not dry_run:
            logger.info("sending batch of {} lenght".format(len(chunck)))
            cursor.executemany(stmt, chunck)
    if not dry_run:
        logger.info("occupancy stored")

def computeEntityOccupancy(entity, entity_type, name, path, refdevice, entityData, previousState, previousTime, hoursDiff):
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    occupancyData = []
    # For each of the hours since the start time given
    for i in range(hoursDiff):
        start_time = (previousTime + datetime.timedelta(
            hours=i))
        end_time = (previousTime + datetime.timedelta(
            hours=(i + 1)))
        hourData = list(filter(
            lambda d: d[1] >= start_time and d[1] < end_time,
            entityData))
        occupiedTime = datetime.timedelta(hours=0)
        if len(hourData) > 0:
            for j in range((len(hourData) + 1)):
                if j != len(hourData):
                    if hourData[j][3]:
                        entity_type = hourData[j][3]
                    if hourData[j][5]:
                        name = hourData[j][5]
                    if hourData[j][6]:
                        refdevice = hourData[j][6]
                timePassed = datetime.timedelta(hours=0)
                if j == 0:
                    timePassed = hourData[j][1] - start_time
                elif j == len(hourData):
                    timePassed = end_time - hourData[j - 1][1]
                else:
                    timePassed = hourData[j][1] - hourData[j - 1][1]
                if previousState == 'occupied':
                    occupiedTime = occupiedTime + timePassed
                if j != len(hourData) and hourData[j][0]:
                    previousState = hourData[j][0]
        if len(hourData) == 0 and previousState == 'occupied':
            occupiedTime = datetime.timedelta(hours=1)
        if len(hourData) == 0 and previousState == 'free':
            occupiedTime = datetime.timedelta(hours=0)
        occupancy = round(math.ceil((occupiedTime.total_seconds() / 3600.0) * 100),2)
        timezonedStartTime = start_time.replace(
            tzinfo=pytz.UTC).isoformat()
        logger.debug("entity {} in path {} occupancy computed is {} "
                     "on time {}".format(entity, path, occupancy,
                                         timezonedStartTime))
        occupancyData.append((occupancy, timezonedStartTime, entity,
                              entity_type, path, name, refdevice))
    return occupancyData


if __name__ == "__main__":
    main(sys.argv[1:])
