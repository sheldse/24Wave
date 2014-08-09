CREATE TABLE gpsdata (
	uid SERIAL PRIMARY KEY,    -- unique id
	client_name VARCHAR(100),  -- client name
	client_ip VARCHAR(15),     -- client ip address
	sender_ip VARCHAR(15),     -- sender ip address
	client_timestamp INTEGER,  -- gps timestamp
	client_lat VARCHAR(16),    -- gps latitude
	client_long VARCHAR(16),   -- gps longitude
	event_type CHAR            -- type of packet
);

CREATE TABLE gpsclientcfg (
	client_name VARCHAR(16),      -- client name
	unicast_port INTEGER,         -- unicast port
	multicast_port INTEGER,       -- multicast port
	multicast_group VARCHAR(15),  -- multicast group
	broadcast_port INTEGER,       -- broadcast port 
	packet_validation CHAR,       -- packet validation
	location_writeival INTEGER,   -- location write interval
	server_host VARCHAR(255),     -- server host
	server_ctlport INTEGER,       -- server control port
	server_retryival INTEGER,     -- server retry interval 
	PRIMARY KEY(client_name)
);
