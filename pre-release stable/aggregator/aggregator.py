#!/usr/bin/env python
#
# GPS Data Aggregation
# Last updated: 30/07/2014
#

import sys
import time
import math
import psycopg2

# Event type
EVENT_LOCAL   = 0
EVENT_UCAST   = 1
EVENT_MCAST   = 2
EVENT_BCAST   = 3
EVENT_ACK     = 4
EVENT_ONLINE  = 7
EVENT_OFFLINE = 8
EVENT_TIMEOUT = 9

# Will be defined on runtime
LAT_PER_METER = 0  # Approx. number of latitude degree per 1 meter
LON_PER_METER = 0  # Approx. number of longitude degree per 1 meter

# Configuration dict's key
config_keys = ( 'coordinate-boundary',
                'corner1-lat', 
                'corner1-long', 
                'corner2-lat',
                'corner2-long', 
                'date-range-use',
                'date-range-start',
                'date-range-end',
                'date-last-hours',
                'client-devices',
                'grid-size',
                'motionless-max-second',
                'pruning-inclusion',
                'record-report',
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
                'dbreport-param-table',
                'dbreport-report-table' )
config = {}                        # Configuration dict
boundary = [ [0, 0], [0, 0] ]      # Configuration boundary
boundary_len = [ 0, 0]             # Configuration boundary x and y length
adj_boundary = [ [0, 0], [0, 0] ]  # Adjusted boundary
adj_boundary_len = [ 0, 0]         # Adjusted boundary x and y length
grid_num = [ 0, 0 ]                # Numbers of grid box in length (x) and width (y)
box_corners = []                   # North-West coordinate and data of each grid box
date_range = [ 0, 0]               # Data range [ start_date, end_date ]

# Trim any spaces and tabs
def trim_spaces(line):
    tmp = line.rsplit(' ')[-1]
    tmp = tmp.rsplit('\t')[-1]
    tmp = tmp.rsplit('\n')[0]
    tmp = tmp.rsplit('\r')[0]
    return tmp

# Default configuration
def default_config():
    config['coordinate-boundary'] = '0.000000'
    config['corner1-lat'] = '0.000000'
    config['corner1-long'] = '0.000000'
    config['corner2-lat'] = '0.000000'
    config['corner2-long'] = '0.000000'
    config['date-range-use'] = 'yes'
    config['date-range-start'] = '0000-00-00,00:00'
    config['date-range-end'] = '0000-00-00,00:00'
    config['date-last-hours'] = '1'
    config['client-devices'] = 'client'
    config['grid-size'] = '10'
    config['motionless-max-second'] = '10'
    config['pruning-inclusion'] = 'yes'
    config['record-report'] = 'yes'
    config['dbgps-host'] = 'localhost'
    config['dbgps-port'] = '5432'
    config['dbgps-user'] = 'postgres'
    config['dbgps-passwd'] = 'postgres'
    config['dbgps-name'] = 'dbgpsname'
    config['dbgps-table'] = 'gpsdata'
    config['dbreport-host'] = 'localhost'
    config['dbreport-port'] = '5432'
    config['dbreport-user'] = 'postgres'
    config['dbreport-passwd'] = 'postgres'
    config['dbreport-name'] = 'dbreportname'
    config['dbreport-param-table'] = 'reportparam'
    config['dbreport-report-table'] = 'reportdata'

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
    return True

# Convert time string 'YYYY-MM-DD,HH:MM' to epoch (UTC)
def strtime_toepoch(strtime):
    l = strtime.split(',')
    if len(l) != 2:
        return None
    date = l[0].split('-')
    if len(date) != 3:
        return None
    hour = l[1].split(':')
    if len(hour) != 2:
        return None
    s = '%.2i-%.2i-%.2i %.2i:%.2i:00' % (int(date[0]), int(date[1]), int(date[2]), 
                                         int(hour[0]), int(hour[1]))
    t = int(time.mktime(time.strptime(s, '%Y-%m-%d %H:%M:%S'))) - time.timezone
    return t

# Check date
def check_date():
    if (config['date-range-use'] == 'yes'):
        date_range[0] = strtime_toepoch(config['date-range-start'])
        if (date_range[0] == None):
            raise BaseException('Invalid date-range-start')
        date_range[1] = strtime_toepoch(config['date-range-end'])
        if (date_range[1] == None):
            raise BaseException('Invalid date-range-end')
    else:
        last_hour = 0
        try:
            last_hour = int(config['date-last-hours'])
        except ValueError:
            raise BaseException('Invalid date-last-hours')
        if (last_hour <= 0):
            raise BaseException('Invalid date-last-hours')
        t = time.time()
        date_range[0] = t - (3600 * last_hour)
        date_range[1] = t
    date_range[0] = int(date_range[0])
    date_range[1] = int(date_range[1])
    if (date_range[0] >= date_range[1]):
        raise BaseException('date-range-start >= date-range-end')

# Validate boundary in configuration
# where, X1 < X4 and Y1 > Y4
def check_boundary():
    if float(config['corner1-lat']) >= float(config['corner2-lat']):
        raise BaseException('corner1-lat >= corner2-lat')
    if float(config['corner1-long']) <= float(config['corner2-long']):
        raise BaseException('corner1-long <= corner2-long')
    return None

# Haversine implementation
# Returns distance in kilometers
def gps_distance(org, dst):
    lat1, lon1 = org
    lat2, lon2 = dst
    radius = 6371 # km
 
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d

# Adjust given boundary's length and width to be filled by grid boxes to a new boundary
def adjust_boundary():
    ideal_length = 0.0
    ideal_width = 0.0
    grid_x_num = 0
    grid_y_num = 0
    grid = float(config['grid-size'])
    x1 = float(config['corner1-lat'])
    y1 = float(config['corner1-long'])
    x2 = float(config['corner2-lat'])
    y2 = float(config['corner2-long'])
    x = 1000 * gps_distance((x1, y1), (x2, y1)) # length in meter, from north-west to north-east
    y = 1000 * gps_distance((x1, y1), (x1, y2)) # width in meter, from north-west to south-west

    # Find ideal length and width to pack grid boxes evenly
    while ideal_length <= x:
        ideal_length += grid
        grid_x_num += 1
    # Expand new length to east
    new_x = (ideal_length - x) * LAT_PER_METER + x2

    # Find ideal length and width to pack grid boxes evenly
    while ideal_width <= y:
        ideal_width += grid
        grid_y_num += 1
    # Expand new width to south
    new_y = y2 - (ideal_width - y) * LON_PER_METER

    # Set grid boxes number
    grid_num[0] = grid_x_num
    grid_num[1] = grid_y_num

    # Set boundary length
    boundary_len[0] = round(x, 3)
    boundary_len[1] = round(y, 3)

    # Set adjusted boundary coordinates
    new_x = round(new_x, 6)
    new_y = round(new_y, 6)
    adj_boundary[0] = [ x1, y1 ]
    adj_boundary[1] = [ new_x, new_y ]
    adj_boundary_len[0] = 1000 * gps_distance((x1, y1), (new_x, y1))
    adj_boundary_len[1] = 1000 * gps_distance((x1, y1), (x1, new_y))
    adj_boundary_len[0] = round(adj_boundary_len[0], 3)
    adj_boundary_len[1] = round(adj_boundary_len[1], 3)

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

def dbgps_query(conn, clients):
    sql = 'SELECT client_name,client_timestamp,client_lat,client_long,event_type '\
          'FROM {0} WHERE client_timestamp >= {1} AND client_timestamp <= {2} AND '\
          'CAST(client_lat AS FLOAT) >= {3} AND CAST(client_lat AS FLOAT) <= {4} AND '\
          'CAST(client_long AS FLOAT) >= {5} AND CAST(client_long AS FLOAT) <= {6} '\
          'AND client_lat != \'\' AND client_long != \'\''\
          .format(config['dbgps-table'], date_range[0], date_range[1], adj_boundary[0][0], 
          adj_boundary[1][0], adj_boundary[1][1], adj_boundary[0][1])

    for i in range(0, len(clients)):
        if i == 0:
            sql = sql + ' AND (client_name=\'{0}\''.format(clients[i])
        else:
            sql = sql + ' OR client_name=\'{0}\''.format(clients[i])
    sql = sql + ') ORDER BY client_name,client_timestamp'

    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    ret = cur.fetchall()
    cur.close()
    return ret

# Get next report id from paramater table
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
        ret = 1000
    else:
        ret = int(res[0][0]) + 1
    return ret

# Insert reporting data to report table
def dbreport_insertreport(conn, report_id, res):
    sql = 'INSERT INTO %s VALUES (%i,%i,%f,%f,%f,%f,%i,%i,%i,%i,%i)' % \
          (config['dbreport-report-table'], report_id,  int(config['grid-size']),
           res[0][0], res[0][1], res[1][0], res[1][1], res[2], res[3], res[4], res[5], res[6])
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    cur.close()

# Insert parameter data to param table
def dbreport_insertparam(conn, report_id, reused_report_id):
    client_list = ''
    for i in range(len(clients)):
        if i == 0: client_list = clients[0]
        else: client_list += ',' + clients[i]
    sql = "INSERT INTO %s VALUES (%i,%i,%i,'%s','%s',%i,%i,%f,%f,%f,%f,'%s',%i,%i,%i,%s)" % \
          (config['dbreport-param-table'], report_id, reused_report_id,
           1 if config['date-range-use'] == 'yes' else 0,
           config['date-range-start'], config['date-range-end'],
           int(config['date-last-hours']),
           1 if config['coordinate-boundary'] == 'yes' else 0,
           float(config['corner1-lat']), float(config['corner1-long']),
           float(config['corner2-lat']), float(config['corner2-long']),
           client_list, int(config['grid-size']),
           int(config['motionless-max-second']),
           1 if config['pruning-inclusion'] == 'yes' else 0,
           'CURRENT_TIMESTAMP')
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    cur.close()

# Get parameter data from param table
def dbreport_getparam(conn, report_id):
    res = []
    sql = 'SELECT * FROM %s WHERE report_id=%i' % \
          (config['dbreport-param-table'], report_id)
    cur = conn.cursor()
    try:
        res = cur.execute(sql)
    except psycopg2.ProgrammingError as e:
        raise BaseException(e.message)
    res = cur.fetchone()
    cur.close()
    if (res is None or len(res) == 0):
        msg = 'Report ID {0} was not found for reuse'.format(report_id)
        raise BaseException(msg)
    config['date-range-use'] = 'yes' if res[2] == 1 else 'no'
    config['date-range-start'] = str(res[3])
    config['date-range-end'] = str(res[4])
    config['date-last-hours'] = str(res[5])
    config['coordinate-boundary'] = 'yes' if res[6] == 1 else 'no'
    config['corner1-lat'] = str(res[7])
    config['corner1-long'] = str(res[8])
    config['corner2-lat'] = str(res[9])
    config['corner2-long'] = str(res[10])
    config['client-devices'] = str(res[11])
    config['grid-size'] = str(res[12])
    config['motionless-max-second'] = str(res[13])
    config['pruning-inclusion'] = 'yes' if res[14] == 1 else 'no'

def get_boxcorner(pos):
    grid = int(config['grid-size'])
    x = adj_boundary[0][0]
    y = adj_boundary[0][1]

    while x < pos[0]:
        x += (grid * LAT_PER_METER)
    if x > pos[0]:
        x -= (grid * LAT_PER_METER)
    while y > pos[1]:
        y -= (grid * LON_PER_METER)
    if (y < pos[1]):
        y += (grid * LON_PER_METER)
    return (x, y)

def find_corners(result):
    grid = int(config['grid-size'])
    motionless_max = int(config['motionless-max-second'])
    motionless_cnt = 0
    pruning_inclusion = config['pruning-inclusion'] == 'yes'
    isonline = True
    event_type = 0
    pos = [ [0, 0], [0, 0] ] # Previous and current position for motionless
    tsp = [ 0, 0 ] # Previous and current timestamp for motionless
    res = []
    corners = {}
    i = 0

    for row in result:
        lat = float(row[2])
        lon = float(row[3])
        event_type = int(row[4])
        # Check if this position is inside the adjusted boundary
        if (lat < adj_boundary[0][0] or lon > adj_boundary[0][1]):
            continue
        if (lat > adj_boundary[1][0] or lon < adj_boundary[1][1]):
            continue

        # Check motionless for all related events
        if motionless_max > 0:
            if (event_type == EVENT_LOCAL or
                event_type == EVENT_UCAST or
                event_type == EVENT_MCAST or
                event_type == EVENT_BCAST or
                event_type == EVENT_ACK):
                pos[1][0] = lat
                pos[1][1] = lon
                tsp[1] = int(row[1])
                if (pos[0][0] == pos[1][0]) and (pos[0][1] == pos[1][1]):
                    motionless_cnt += abs(tsp[1] - tsp[0]) # Update motionless counter
                else:
                    motionless_cnt = 0 # Reset motionless counter
                pos[0][0] = lat
                pos[0][1] = lon
                tsp[0] = tsp[1]
                if (motionless_cnt >= motionless_max):
                    continue

        # When pruning inclusion is disabled, EVENT_LOCATION will be discarded when
        # EVENT_OFFLINE is found, and will be included back until EVENT_ONLINE is found
        if (pruning_inclusion is False):
            if (event_type == EVENT_TIMEOUT):
                isonline = False
            elif (event_type == EVENT_ONLINE):
                isonline = True
            if (isonline == False and event_type == EVENT_LOCAL):
                continue

        # Get box corner based on latitude and longitude
        c = get_boxcorner((lat, lon))
        key = '%.6f,%.6f' % (c[0], c[1])
        if (corners.has_key(key) is False):
            # Set corner2 position
            corner2_x = c[0] + (grid * LAT_PER_METER)
            corner2_y = c[1] - (grid * LON_PER_METER)
            # [ (corner2_x, corner2_y), EVENT_LOCAL, EVENT_UCAST, EVENT_BCAST, EVENT_MCAST, EVENT_ACK ]
            corners[key] = [ (corner2_x, corner2_y), 0, 0, 0, 0, 0 ]
        if event_type == EVENT_LOCAL: corners[key][1] += 1
        elif event_type == EVENT_UCAST: corners[key][2] += 1
        elif event_type == EVENT_BCAST: corners[key][3] += 1
        elif event_type == EVENT_MCAST: corners[key][4] += 1
        elif event_type == EVENT_ACK: corners[key][5] += 1

    # Insert item from dictionary to result
    for c in corners:
        total = 0
        for i in range(1, 6):
            total += corners[c][i]
        if total > 0:
            pos = c.rsplit(',')
            res.append([ (float(pos[0]), float(pos[1])), (corners[c][0][0], corners[c][0][1]),
                       corners[c][1], corners[c][2], corners[c][3], corners[c][4], corners[c][5] ])
    return res


# Main routine

dbgps_conn = []
dbreport_conn = []
timing = [0, 0]
report_id = 0
reused_report_id = 0

# Check arguments
if len(sys.argv) < 2:
    progname = sys.argv[0]
    i = progname.rfind('/')
    if i >= 0:
            i += 1
            progname = progname[i:]
    print 'Usage: {0} <config-file> [report-id]'.format(progname)
    sys.exit(-1)

# Set default configuration
default_config()
# Read configuration
if read_config(sys.argv[1]) is False:
    print 'Unable to read configuration file'
    sys.exit(-1)

if len(sys.argv) >= 3:
    r = sys.argv[2].isdigit()
    if r is False:
        print 'Invalid report ID for reuse'
        sys.exit(-1)
    r = int(sys.argv[2])
    try:
        conn = dbreport_connect()
        dbreport_getparam(conn, r)
    except BaseException as e:
        print e
        sys.exit(-1)
    reused_report_id = r
    conn.close()
    print 'Reusing report ID {0}\n'.format(r)

# Parse client 
clients = config['client-devices'].rsplit(',')
if len(clients) == 0:
    print 'No client was defined'
    sys.exit(-1)

# Check boundary
try:
    check_boundary()
except BaseException as e:
    print 'Invalid boundary: ' + e.message
    sys.exit(-1)

# Set latitude and longitude per meter value
LAT_PER_METER = 0.00001
LON_PER_METER = 1 / (111111 * math.cos(math.radians(float(config['corner1-lat']))))

# Adjust boundary to evenly contains grid boxes
adjust_boundary()
print 'Boundary: corner1={0},{1}  corner2={2},{3} length={4}m width={5}m'\
      .format(config['corner1-lat'], config['corner1-long'], 
      config['corner2-lat'], config['corner2-long'],
      boundary_len[0], boundary_len[1])
print 'Adjusted Boundary: corner1={0},{1}  corner2={2},{3} length={4}m width={5}m'.\
      format(adj_boundary[0][0], adj_boundary[0][1], adj_boundary[1][0], 
      adj_boundary[1][1], adj_boundary_len[0], adj_boundary_len[1])
print 'Grid Box: size={0}m x={1} y={2} total={3}'.\
      format(config['grid-size'], grid_num[0], grid_num[1], grid_num[0] * grid_num[1])

# Validate date range
try:
    check_date()
except BaseException as e:
    print 'Invalid date: ' + e.message
    sys.exit(-1)

print 'Time: mode={0} start={1} end={2}'.format(
      'hours-range' if config['date-range-use'] == 'yes' else 'last-hours',
      date_range[0], date_range[1])

print 'Clients:',
for c in clients:
    print c,
print ''

print 'Max. Motionless: {0} second(s)'.format(config['motionless-max-second'])
print 'Pruning Inclusion: {0}'.format('yes' if config['pruning-inclusion'] == 'yes' else 'no')
print ''

# Connect to GPS data database
timing[0] = time.time()
conn = []
report = []
rows = []

try:
    conn = dbgps_connect()
except BaseException as e:
    print e
    sys.exit(-1)

print 'Running query...',
sys.stdout.flush()
try:
    rows = dbgps_query(conn, clients)
except BaseException as e:
    print e
    sys.exit(-1)
print 'done {0} row(s)\n'.format(len(rows))
sys.stdout.flush()

if len(rows) == 0:
    print 'Empty row\n'
    sys.exit(1)

print 'Processing...'
res = find_corners(rows)
if len(res) == 0:
    print 'Empty record\n'
else:
    idx = 1
    total = 0
    for n in res:
        print '#{0} corner1={1},{2} corner2={3},{4} local={5} ucast={6} bcast={7} '\
              'mcast={8} ack={9}'.format(idx, n[0][0], n[0][1], n[1][0], n[1][1], 
              n[2], n[3], n[4], n[5], n[6])
        for i in range(2, 7):
            total += n[i]
        idx += 1
        # Append to report
        report.append(n)
    print 'Total event: {0}\n'.format(total)
conn.close()
timing[1] = time.time()
print 'Total Time: {0} second(s)'.format(round(timing[1] - timing[0], 2))

# Connect to report database and record report
if (config['record-report'] == 'yes'):
    try:
        conn = dbreport_connect()
    except BaseException as e:
        print e
        sys.exit(-1)

    # Get report id
    try:
        report_id = dbreport_getid(conn)
    except BaseException as e:
        print e
        sys.exit(-1)

    print '\nReport ID:', report_id

    # Insert and insert result
    print 'Inserting report data...',
    for res in report:
        try:
            dbreport_insertreport(conn, report_id, res)
        except BaseException as e:
            print e
            sys.exit(-1)
    print ''

    # Insert parameters
    print 'Inserting report parameters...',
    try:
        dbreport_insertparam(conn, report_id, reused_report_id)
    except BaseException as e:
        print e
        sys.exit(-1)
    conn.commit()
    conn.close()
    print ''
else:
    print 'Not recording reports'

sys.exit(0)
