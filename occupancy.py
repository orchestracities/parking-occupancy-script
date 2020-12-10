#!/usr/bin/python

import datetime
import getopt
import logging
import math
import pytz
import sys

from crate import client
from dateutil import parser


def main(argv):
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Setting up the connection to CrateDB (with command line args)
    crate_host = None
    crate_user = None
    crate_password = None
    start_date = None
    end_date = None
    delta = 24
    tenant_name = 'EKZ'
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
        print('occupancy.py -h <cratedb_host> -u <cratedb_user>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            crate_host = arg
        elif opt in ("-u", "--user"):
            crate_user = arg
        elif opt in ("-p", "--password"):
            crate_password = arg
        elif opt in ("-t", "--tenant-name"):
            tenant_name = parser.parse(arg)
        elif opt in ("-s", "--start-date"):
            start_date = parser.parse(arg)
        elif opt in ("-e", "--end-date"):
            end_date = parser.parse(arg)
        elif opt in ("-d", "--delta-time"):
            delta = int(arg)
        elif opt in ("-r", "--dry-run"):
            dry_run = bool(arg)
    if not crate_host or not crate_user:
        logger.error("missing parameters")
        print('occupancy.py -h <cratedb_host> -u <cratedb_user>')
        sys.exit(2)
    try:
        logger.info("connecting...")
        schema = "mt" + tenant_name.lower()
        connection = client.connect(crate_host, username=crate_user,
                                    password=crate_password)
        cursor = connection.cursor()
        computeOccupancy(cursor, schema, start_date, end_date, delta, dry_run)
    except Exception as e:
        logger.error(str(e), exc_info=True)
        sys.exit(2)
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
        currentTime = datetime.datetime.now().replace(microsecond=0, second=0,
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
        sys.exit(2)

    # Setting up cursor and pulling data since the start time
    limit = 1000
    offset = 0
    data = []
    query = 'SELECT "status","time_index", "entity_id", "entity_type", ' \
           '"fiware_servicepath", "name", "refdevice" FROM ' \
           '"{}"."etparkingspot" WHERE "time_index">=? AND "time_index"<=? ' \
           'AND status!=? AND status!=? ORDER BY time_index ASC LIMIT {} ' \
           'OFFSET {}'
    while offset >= 0:
        stmt = query.format(schema, limit, offset)
        cursor.execute(stmt,
            (previousTime.strftime('%s') + '000',
             currentTime.strftime('%s') + '000', 'None', 'unknown'))
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
        pathData = filter(lambda a: a[4] == path, data)
        entityIds = list(dict.fromkeys(map(lambda a: a[2], pathData)))
        for entity in entityIds:
            # Getting the last known status. Assumed free if none found
            stmt = 'SELECT "status","time_index", "entity_id", ' \
                   '"entity_type", "fiware_servicepath", "name", "refdevice" ' \
                   'FROM "{}"."etparkingspot" WHERE "time_index"<? AND ' \
                   '"entity_id"=? AND "fiware_servicepath"=? AND status!=? ' \
                   'AND status!=? ORDER BY time_index DESC LIMIT 1'.format(schema)

            cursor.execute(
                stmt,
                (previousTime.strftime('%s') + '000', entity, path, 'None',
                 'unknown'))
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
            entityData = filter(lambda a: a[2] == entity, pathData)
            # For each of the hours since the start time given
            for i in range(hoursDiff):
                start_time = (currentTime - datetime.timedelta(
                    hours=(hoursDiff - i + 1))).strftime('%s') + '000'
                end_time = (currentTime - datetime.timedelta(
                    hours=(hoursDiff - i))).strftime('%s') + '000'
                hourData = filter(
                    lambda d: d[1] >= int(start_time) and d[1] < int(end_time),
                    entityData)
                occupiedTime = 0
                if len(hourData) > 0:
                    for j in range((len(hourData) + 1)):
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
                            timePassed = long(end_time) - hourData[j - 1][1]
                        else:
                            timePassed = hourData[j][1] - hourData[j - 1][1]
                        if previousState == 'occupied':
                            occupiedTime = occupiedTime + timePassed
                        if j != len(hourData) and hourData[j][0]:
                            previousState = hourData[j][0]
                if len(hourData) == 0 and previousState == 'occupied':
                    occupiedTime = 3600000
                if len(hourData) == 0 and previousState == 'free':
                    occupiedTime = 0
                occupancy = int(math.ceil((occupiedTime / 3600000.0) * 100))
                timezonedStartTime = datetime.datetime.fromtimestamp(
                    long(start_time) / 1000.0).strftime('%s') + '000'
                logger.debug("entity {} in path {} occupancy computed is {} "
                             "on time {}".format(entity, path, occupancy,
                                                 timezonedStartTime))
                occupancyData.append((occupancy, timezonedStartTime, entity,
                                      entity_type, path, name, refdevice))
    logger.info("occupancy computed")

    if dry_run:
        logger.info("dry run mode, no data will be stored")
    stmt = 'INSERT INTO "{}"."etparkingoccupancy" (occupancy, time_index, entity_id, entity_type, fiware_servicepath, name, refdevice) VALUES (?,?,?,?,?,?,?)'.format(schema)
    for i in range(0, len(occupancyData), 1000):
        chunck = occupancyData[i:i + 1000]
        if not dry_run:
            logger.info("sending batch of {} lenght".format(len(chunck)))
            cursor.executemany(stmt, chunck)
    if not dry_run:
        logger.info("occupancy stored")


if __name__ == "__main__":
    main(sys.argv[1:])
