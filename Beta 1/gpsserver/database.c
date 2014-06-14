#include <libpq-fe.h>
#include <stdio.h>
#include "utils.h"
#include "database.h"
#include "config.h"
#include "msg.h"

typedef PGconn dbctx_t;

dbctx_t *db_connect(void)
{
	dbctx_t *ctx;
	char conn_str[128];

	snprintf(conn_str, sizeof(conn_str),
		 "hostaddr=%s port=%i dbname=%s user=%s password=%s connect_timeout=3",
		 config.db_host, config.db_port, config.db_name, config.db_user, 
		 config.db_passwd);

	ctx = PQconnectdb(conn_str);
	if (PQstatus(ctx) != CONNECTION_OK) {
		debug(DEBUG_ERROR, "could not connect to database: %s", PQerrorMessage(ctx));
		PQfinish (ctx);
		return NULL;
	}
	return ctx;
}

void db_close(dbctx_t *ctx)
{
	PQfinish(ctx);
}

int db_insertctl(dbctx_t *ctx,
		 const char *name,
		 const char *addr,
		 int event)
{
	PGresult *result;
	int ret;
	long int tsp;
	char cmd[1024];
	char ival[8];

	/* Write config.packet_interval to client_lat when event = EVENT_ONLINE */
	if (event == 7)
		snprintf(ival, sizeof(ival), "%i", config.packet_interval);
	tsp = time(NULL);
	snprintf(cmd, sizeof(cmd),
		 "INSERT INTO %s(client_name,client_ip,client_timestamp,client_lat,event_type) "
		 "VALUES('%s','%s',%li,'%s',%i)", config.db_table, name, addr, tsp,
		 event == 7 ? ival : "", event);

	result = PQexec(ctx, cmd);
	if (result == NULL) {
		debug(DEBUG_ERROR, "%s", PQerrorMessage (ctx));
		return 0;
	}
	if (PQresultStatus(result) == PGRES_COMMAND_OK)
		ret = 1;
	else {
		debug(DEBUG_ERROR, "%s", PQresultErrorMessage (result));
		ret = 0;
	}
	PQclear(result);
	return ret;
}

int db_insertack(dbctx_t *ctx,
		 const struct ack_msg *ack,
		 const char *addr,
		 int event)
{
	PGresult *result;
	int ret;
	char cmd[1024];

	snprintf(cmd, sizeof(cmd),
		 "INSERT INTO %s(client_name,client_ip,client_timestamp,client_lat,"
		 "client_long,event_type) VALUES('%s','%s',%u,'%s','%s',%i)",
		 config.db_table, ack->name, addr, ack->tsp, ack->latitude,
		 ack->longitude, event);

	result = PQexec(ctx, cmd);
	if (result == NULL) {
		debug(DEBUG_ERROR, "%s", PQerrorMessage (ctx));
		return 0;
	}
	if (PQresultStatus(result) == PGRES_COMMAND_OK)
		ret = 1;
	else {
		debug(DEBUG_ERROR, "%s", PQresultErrorMessage (result));
		ret = 0;
	}
	PQclear(result);
	return ret;
}
