#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils.h"
#include "database.h"
#include "config.h"

typedef PGconn dbctx_t;

dbctx_t *db_connect(void)
{
	dbctx_t *ctx;
	char conn_str[128];

	snprintf(conn_str, sizeof(conn_str),
		 "hostaddr=%s port=%i dbname=%s user=%s password=%s connect_timeout=3",
		 config.db_addr, config.db_port, config.db_name, config.db_user, 
		 config.db_passwd);

	ctx = PQconnectdb(conn_str);
	if (PQstatus (ctx) != CONNECTION_OK) {
		debug(DEBUG_ERROR, "could not connect to database: %s", PQerrorMessage (ctx));
		PQfinish (ctx);
		return NULL;
	}
	return ctx;
}

void db_close(dbctx_t *ctx)
{
	PQfinish(ctx);
}

int db_insert(dbctx_t *ctx,
	      const struct db_data *data)
{
	PGresult *result;
	int ret;
	char cmd[1024];
	char gps_lat[16];
	char gps_lon[16];

	sprintf(gps_lat, "%f", data->gps_lat);
	sprintf(gps_lon, "%f", data->gps_lon);
	snprintf(cmd, sizeof(cmd),
		 "insert into %s(client_name,client_ip,sender_ip,client_timestamp,client_lat,"
		 "client_long,event_type) values('%s','%s','%s',%li,'%s','%s',%i)", 
		 config.db_tabledata, data->client_name, data->client_ip, data->sender_ip,
		 (long) data->gps_tsp, gps_lat, gps_lon, data->packet_type);
	result = PQexec (ctx, cmd);
	if (result == NULL) {
		debug(DEBUG_ERROR, "%s", PQerrorMessage (ctx));
		return 0;
	}
	if (PQresultStatus (result) == PGRES_COMMAND_OK)
		ret = 1;
	else {
		debug(DEBUG_ERROR, "could not insert to db: %s", PQresultErrorMessage (result));
		ret = 0;
	}
	PQclear (result);
	return ret;
}

int db_getcfg(dbctx_t *ctx,
	      struct db_config *cfg)
{
	PGresult *result;
	struct db_config tcfg;
	char cmd[128];
	int row;

	snprintf(cmd, sizeof(cmd), "SELECT * FROM %s WHERE client_name='%s'",
		 config.db_tablecfg, config.client_name);
	result = PQexec(ctx, cmd);
	if (result == NULL || PQresultStatus (result) != PGRES_TUPLES_OK) {
		debug(DEBUG_ERROR, "%s", PQerrorMessage(ctx));
		return -1;
	}

	row = PQntuples(result);
	if (row <= 0) {
		PQclear(result);
		return 0;
	}

	snprintf(tcfg.name, sizeof(tcfg.name), "%s", PQgetvalue(result, 0, 0));
	tcfg.ucast_port = atoi(PQgetvalue(result, 0, 1));
	tcfg.mcast_port = atoi(PQgetvalue(result, 0, 2));
	snprintf(tcfg.mcast_group, sizeof(tcfg.mcast_group), "%s", PQgetvalue(result, 0, 3));
	tcfg.bcast_port = atoi(PQgetvalue(result, 0, 4));
	tcfg.packet_validation = atoi(PQgetvalue(result, 0, 5));
	tcfg.location_writeival = atoi(PQgetvalue(result, 0, 6));
	snprintf(tcfg.server_ip, sizeof(tcfg.server_ip), "%s", PQgetvalue(result, 0, 7));
	tcfg.server_ctlport = atoi(PQgetvalue(result, 0, 8));
	tcfg.server_retryival = atoi(PQgetvalue(result, 0, 9));

	memcpy(cfg, &tcfg, sizeof(tcfg));
	PQclear(result);
	return 1;
}

void db_debugcfg(const struct db_config *cfg)
{
	debug(DEBUG_INFO, "client-name='%s'", cfg->name);
	debug(DEBUG_INFO, "ucast-port=%i mcast-port=%i mcast-group=%s bcast-port=%i",
	      cfg->ucast_port, cfg->mcast_port, cfg->mcast_group, cfg->bcast_port);
	debug(DEBUG_INFO, "packet-validation=%s", cfg->packet_validation ? "yes" : "no");
	debug(DEBUG_INFO, "server-ip=%s server-port=%i", cfg->server_ip, cfg->server_ctlport);
	debug(DEBUG_INFO, "location-writeival=%ims", cfg->location_writeival);
	debug(DEBUG_INFO, "server-retryival=%ims", cfg->server_retryival);
}
