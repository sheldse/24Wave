CREATE TABLE reportparam (
    report_id INTEGER PRIMARY KEY,
    reused_report_id INTEGER,
    date_range_use INT,
    date_range_start VARCHAR(16),
    date_range_end VARCHAR(16),
    date_last_hours INTEGER,
    coordinate_boundary INTEGER,
    corner1_lat FLOAT,
    corner1_long FLOAT,
    corner2_lat FLOAT,
    corner2_long FLOAT,
    client_devices VARCHAR(1024),
    grid_size INTEGER,
    motionless_max_second INTEGER,
    pruning_inclusion INTEGER,
    run_time TIMESTAMP(0) WITHOUT TIME ZONE
);

CREATE TABLE reportdata (
    report_id INTEGER,
    grid_size INTEGER,
    corner1_lat FLOAT,
    corner1_long FLOAT,
    corner2_lat FLOAT,
    corner2_long FLOAT,
    location_total INTEGER,
    unicast_total INTEGER,
    broadcast_total INTEGER,
    multicast_total INTEGER,
    ack_total INTEGER
);
