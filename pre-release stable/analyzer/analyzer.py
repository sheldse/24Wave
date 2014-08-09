#!/usr/bin/env python
#
# GPS Data Analyzer
# Last updated: 08/04/2014
#

import sys
import os
import time
import math
import psycopg2
from datetime import date, timedelta

# Event type
EVENT_LOCAL   = 0
EVENT_UCAST   = 1
EVENT_MCAST   = 2
EVENT_BCAST   = 3
EVENT_ACK     = 4
EVENT_ONLINE  = 7
EVENT_OFFLINE = 8
EVENT_TIMEOUT = 9

# Configuration dict's key
config_keys = ( 'aggregator-script',
                'aggregator-config',
                'previous-days',
                'dbhost',
                'dbport',
                'dbuser',
                'dbpasswd',
                'dbname',
                'dbgeotable',
                'dbclienttable',
                'dbgps-host',
                'dbgps-port',
                'dbgps-user',
                'dbgps-passwd',
                'dbgps-name',
                'dbgps-table',
                'dbreport-host',
                'dbreport-port',
                'dbreport-user',
                'dbreport-passwd',
                'dbreport-name',
                'dbreport-name',
                'dbreport-param-table',
                'dbreport-report-table',
                'corner1-lat',
                'corner1-long',
                'corner2-lat',
                'corner2-long' )

aggrconfig_file = '/tmp/gps-analyzer-aggregator.conf'  # File path for the rewritten aggregator configuration
config = {}  # Configuration dict

# Trim any spaces and tabs
def trim_spaces(line):
    tmp = line.rsplit(' ')[-1]
    tmp = tmp.rsplit('\t')[-1]
    tmp = tmp.rsplit('\n')[0]
    tmp = tmp.rsplit('\r')[0]
    return tmp

# Default configuration
def default_config():
    config['aggregator-script'] = './aggregator.py'
    config['aggregator-config'] = './aggregator.conf'
    config['previous-days'] = '1'
    config['dbhost'] = 'localhost'
    config['dbport'] = '5432'
    config['dbuser'] = 'postgres'
    config['dbpasswd'] = 'postgres'
    config['dbname'] = 'analysis'
    config['dbgeotable'] = 'geoanalysis'
    config['dbclienttable'] = 'clientanalysis'
    config['dbgps-host'] = 'localhost'
    config['dbgps-port'] = '5432'
    config['dbgps-user'] = 'postgres'
    config['dbgps-passwd'] = 'postgres'
    config['dbgps-name'] = 'test'
    config['dbgps-table'] = 'gpsdata'
    config['dbreport-host'] = 'localhost'
    config['dbreport-port'] = '5432'
    config['dbreport-user'] = 'postgres'
    config['dbreport-passwd'] = 'postgres'
    config['dbreport-name'] = 'dbreportname'
    config['dbreport-param-table'] = 'reportparam'
    config['dbreport-report-table'] = 'reportdata'
    config['corner1-lat'] = '0.00000'
    config['corner1-long'] = '0.00000'
    config['corner2-lat'] = '0.00000'
    config['corner2-long'] = '0.00000'

# Read configuration
def read_config(filename):
    try:
        f = open(filename, 'r')
    except IOError:
        return False
    for line in f:
        if line[0] == '#' or line[0] == '\n': # Skip comments and new lines
            continue
        for key in config_keys:
            i = line.rfind(key)
            if i >= 0:
                config[key] = trim_spaces(line)
    f.close()
    return True

# Rewrite aggregator configuration and modify clients and date range option
def rewrite_aggrconfig(clients, date_range):
    try:
        old = open(config['aggregator-config'], 'r')
        new = open(aggrconfig_file, 'w')
    except IOError:
        return False
    for line in old:
        if line[0] == '#' or line[0] == '\n': # Skip comments and new lines
            continue
        new.write(line)
    # Write clients and time range
    new.write('date-range-use yes\n')
    new.write('date-range-start {0}\n'.format(date_range[0]))
    new.write('date-range-end {0}\n'.format(date_range[1]))
    line = 'client-devices '
    for c in clients:
        line += c
        if c != clients[-1]:
            line += ','
    new.write(line + '\n')
    old.close()
    new.close()
    return True

# Returns local date range in previous day in UTC
def get_daterange():
    prev = date.today() - timedelta(days=int(config['previous-days']), hours=0, minutes=0)
    fmt1 = prev.strftime('%Y-%m-%d 00:00:00')
    t1 = time.mktime(time.strptime(fmt1, '%Y-%m-%d %H:%M:%S'))
    fmt2 = prev.strftime('%Y-%m-%d 23:59:59')
    t2 = time.mktime(time.strptime(fmt2, '%Y-%m-%d %H:%M:%S'))
    return ( time.strftime('%Y-%m-%d,%H:%M', time.gmtime(t1)),
             time.strftime('%Y-%m-%d,%H:%M', time.gmtime(t2)) )

# Connect to GPS data database
def dbgps_connect():
    connstr = 'host={0} port={1} dbname={2} user={3} password={4} connect_timeout=10'\
              .format(config['dbgps-host'], config['dbgps-port'], config['dbgps-name'],
              config['dbgps-user'], config['dbgps-passwd'])
    try:
        conn = psycopg2.connect(connstr)
    except psycopg2.OperationalError as e:
        raise BaseException(e.message)
    return conn

# Get all available client from GPS data table
def dbgps_getclients(conn):
    result = []
    sql = 'SELECT DISTINCT(client_name) FROM {0}'.format(config['dbgps-table'])
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    ret = cur.fetchall()
    cur.close()
    for c in ret:
        result.append(c[0])
    return result

# Connect to report database
def dbreport_connect():
    connstr = 'host={0} port={1} dbname={2} user={3} password={4} connect_timeout=10'\
              .format(config['dbreport-host'], config['dbreport-port'], config['dbreport-name'],
              config['dbreport-user'], config['dbreport-passwd'])
    try:
        conn = psycopg2.connect(connstr)
    except psycopg2.OperationalError as e:
        raise BaseException(e.message)
    return conn

# Get last report id from paramater table
def dbreport_getid(conn):
    sql = 'SELECT MAX(report_id) FROM {0}'.format(config['dbreport-param-table'])
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    res = cur.fetchall()
    cur.close()
    if res[0][0] == None:
        ret = 0
    else:
        ret = int(res[0][0])
    return ret

# Compile reports from GPS data table
def dbreport_getreport(conn, report_id):
    loc_total = 0
    ucast_total = 0
    bcast_total = 0
    mcast_total = 0
    ack_total = 0

    sql = 'SELECT * FROM {0} WHERE report_id={1}'.\
          format(config['dbreport-report-table'], report_id)
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        loc_total += int(r[6])
        ucast_total += int(r[7])
        bcast_total += int(r[8])
        mcast_total += int(r[9])
        ack_total += int(r[10])
    ucast_pct = float(ucast_total) / loc_total * 100
    bcast_pct = float(bcast_total) / loc_total * 100
    mcast_pct = float(mcast_total) / loc_total * 100
    ack_pct = float(ack_total) / loc_total * 100
    return ( loc_total, ucast_total, bcast_total, mcast_total,
             ack_total, ucast_pct, bcast_pct, mcast_pct, ack_pct )

# Get rolling average from GPS data table
# Ideally it requires 7 days, if we have less than 7 days
# then just divide it against the available days
def dbreport_getrollingavg(conn, client_name=''):
    total_day = 0
    total_ucast = 0
    total_loc = 0
    total_pct = 0.0
    for i in range(0, 7):
        x = date.today() - timedelta(days=i + int(config['previous-days']))
        s1 = x.strftime('%Y-%m-%d 00:00:00')
        s2 = x.strftime('%Y-%m-%d 23:59:59')
        utc1 = int(time.mktime(time.strptime(s1, '%Y-%m-%d %H:%M:%S')) + time.timezone) # Local to UTC
        utc2 = int(time.mktime(time.strptime(s2, '%Y-%m-%d %H:%M:%S')) + time.timezone) # Local to UTC
        # Query unicast total
        sql = "SELECT event_type FROM {0} WHERE event_type='{1}' OR event_type='{2}'\
              AND client_timestamp >= {3} AND client_timestamp <= {4}".\
              format(config['dbgps-table'], EVENT_LOCAL, EVENT_UCAST, utc1, utc2)
        if client_name != '':
            sql += " AND client_name='{0}'".format(client_name)
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except psycopg2.ProgrammingError as e:
            raise BaseException(e.message)
        result = cur.fetchall()
        cur.close()
        if len(result) != 0:
            for row in result:
                if int(row[0]) == EVENT_LOCAL:
                    total_loc += 1
                else: # EVENT_UCAST
                    total_ucast += 1
            total_day += 1
            total_pct += (float(total_ucast) / total_loc * 100)
    if total_day == 0:
        return 0.0
    return total_pct / total_day

# Connect to analysis database
def dbanalysis_connect():
    connstr = 'host={0} port={1} dbname={2} user={3} password={4} connect_timeout=10'\
              .format(config['dbhost'], config['dbport'], config['dbname'],
              config['dbuser'], config['dbpasswd'])
    try:
        conn = psycopg2.connect(connstr)
    except psycopg2.OperationalError as e:
        raise BaseException(e.message)
    return conn

# Insert geographic analysis data
def insert_geoanalysis(conn, report_id, report_data):
    d = date.today() - timedelta(days=int(config['previous-days']), hours=0, minutes=0)
    date_start = d.strftime('%Y-%m-%d 00:00:00')
    date_end = d.strftime('%Y-%m-%d 23:59:59')
    sql = "INSERT INTO %s VALUES(%i,'%s','%s',%i,%f,%f,%f,%f,%i,%i,%i,%i,%i,%f,%f,%f,%f)"\
          % (config['dbgeotable'], report_id, date_start, date_end, 1,
             float(config['corner1-lat']), float(config['corner1-long']),
             float(config['corner2-lat']), float(config['corner2-long']),
             report_data[0], report_data[1], report_data[2], report_data[3],
             report_data[4], report_data[5], report_data[6], report_data[7],
             report_data[8])
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    conn.commit()
    cur.close()

# Insert client analysis data
def insert_clientanalysis(conn, report_id, client_name, report_data):
    d = date.today() - timedelta(days=int(config['previous-days']), hours=0, minutes=0)
    date_start = d.strftime('%Y-%m-%d 00:00:00')
    date_end = d.strftime('%Y-%m-%d 23:59:59')
    sql = "INSERT INTO %s VALUES(%i,'%s','%s','%s',%i,%f,%f,%f,%f,%i,%f,%f,%f,%f)"\
          % (config['dbclienttable'], report_id, client_name, date_start, date_end, 1,
             float(config['corner1-lat']), float(config['corner1-long']),
             float(config['corner2-lat']), float(config['corner2-long']),
             report_data[0], report_data[5], report_data[6], report_data[7],
             report_data[8])
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    conn.commit()
    cur.close()              


#
# Main routine
#
clients = []
date_range = ()

# Check arguments
if len(sys.argv) < 2:
    progname = sys.argv[0]
    i = progname.rfind('/')
    if i >= 0:
            i += 1
            progname = progname[i:]
    print 'Usage: {0} <config-file>'.format(progname)
    sys.exit(1)

# Set default configuration
default_config()
# Read configuration
if read_config(sys.argv[1]) is False:
    print 'Unable to read configuration file: {0}'.format(sys.argv[1])
    sys.exit(1)

# Check previous-days option to has minimal 1 day
if int(config['previous-days']) <= 0:
    config['previous-days'] = '1'

# Read aggregator configuration
if read_config(config['aggregator-config']) is False:
    print 'Unable to read aggregator configuration file: {0}'.format(config['aggregator-config'])
    sys.exit(1)

# Query GPS data
try:
    conn = dbgps_connect()
    clients = dbgps_getclients(conn)
    conn.close()
except BaseException as e:
    print e
    sys.exit(1)

# Date range
date_range = get_daterange()

# Geographic analysis
if rewrite_aggrconfig(clients, date_range) is False:
    print 'Unable to write aggregator configuration file'
    sys.exit(-1)

report_date = date.today() - timedelta(days=int(config['previous-days']))
print 'Report date is {0}'.format(report_date.strftime('%Y-%m-%d'))

exit_status = 0
pid = os.fork()
if pid == 0: # Child
    os.execv(config['aggregator-script'], ( config['aggregator-script'], aggrconfig_file ))
else:
    print 'Running aggregator for geographic analysis with PID {0}...\n'.format(pid)
    ret = os.waitpid(pid, 0)
    exit_status = os.WEXITSTATUS(ret[1])
    if (os.WIFEXITED(ret[1]) is False) or (exit_status < 0):
        print 'Unable to run aggregator'
        sys.exit(1)

# Aggregator was exited with success status
if exit_status == 0:
    try:
        conn = dbreport_connect()
        report_id = dbreport_getid(conn) # Find report ID
    except BaseException as e:
        print e
        sys.exit(-1)

    report_data = dbreport_getreport(conn, report_id)
    conn.close()

    # Insert geographic analysis report
    try:
        conn = dbanalysis_connect()
        insert_geoanalysis(conn, report_id, report_data)
        conn.close()
    except BaseException as e:
        print e
        sys.exit(-1)


# Client analysis
for c in clients:
    if rewrite_aggrconfig(( c, ), date_range) is False:
        print 'Unable to write aggregator configuration'
        sys.exit(-1)
    pid = os.fork()
    if pid == 0: # Child
        os.execv(config['aggregator-script'], ( config['aggregator-script'], aggrconfig_file ))
    else:
        print 'Running aggregator for client analysis with PID {0}...\n'.format(pid)
        ret = os.waitpid(pid, 0)
        exit_status = os.WEXITSTATUS(ret[1])
        if (os.WIFEXITED(ret[1]) is False) or (exit_status < 0):
            print 'Unable to run aggregator'
            sys.exit(1)

        # Exited with success status
        if exit_status == 0:
            try:
                conn = dbreport_connect()
                report_id = dbreport_getid(conn)
                report_data = dbreport_getreport(conn, report_id)
                conn.close()
            except BaseException as e:
                print e
                sys.exit(-1)

            # Insert client analysis report
            try:
                conn = dbanalysis_connect()
                insert_clientanalysis(conn, report_id, c, report_data)
                conn.close()
            except BaseException as e:
                print e
                sys.exit(-1)
        print ''

sys.exit(0)
