CREATE TABLE geoanalysis (
    report_id INTEGER PRIMARY KEY,
    report_start TIMESTAMP(0) WITHOUT TIME ZONE,
    report_end TIMESTAMP(0) WITHOUT TIME ZONE,
    report_type INTEGER,
    corner1_lat FLOAT,
    corner1_long FLOAT,
    corner2_lat FLOAT,
    corner2_long FLOAT,
    location_total INTEGER,
    unicast_total INTEGER,
    broadcast_total INTEGER,
    multicast_total INTEGER,
    ack_total INTEGER,
    unicast_pct FLOAT,
    broadcast_pct FLOAT,
    multicast_pct FLOAT,
    ack_pct FLOAT
);

CREATE TABLE clientanalysis (
    report_id INTEGER PRIMARY KEY,
    client_name VARCHAR(100),
    report_start TIMESTAMP(0) WITHOUT TIME ZONE,
    report_end TIMESTAMP(0) WITHOUT TIME ZONE,
    report_type INTEGER,
    corner1_lat FLOAT,
    corner1_long FLOAT,
    corner2_lat FLOAT,
    corner2_long FLOAT,
    location_total INTEGER,
    unicast_pct FLOAT,
    broadcast_pct FLOAT,
    multicast_pct FLOAT,
    ack_pct FLOAT
);

