#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"
#include "config.h"
#include "database.h"
#include "utils.h"

static sqlite3 *bufdb;
static int bufrun = 0;
static pthread_t bufthread;
static pthread_mutex_t bufmutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t condmutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t bufcond = PTHREAD_COND_INITIALIZER;

static int buffer_delete(unsigned uid)
{
	char *cmd;
	int ret;

	cmd = sqlite3_mprintf("DELETE FROM buffer WHERE uid=%u", uid);
	ret = sqlite3_exec(bufdb, cmd, NULL, NULL, NULL);
	sqlite3_free(cmd);

	if (ret != SQLITE_OK) {
		debug(DEBUG_ERROR, "could not delete buffer: %s",
		      sqlite3_errmsg(bufdb));
		return 0;
	}
	return 1;
}

static void buffer_process(dbctx_t *dbctx)
{
	int ret, row, col;
	int i, j;
	unsigned uid;
	char **table;
	struct db_data dbdata;

	ret = sqlite3_get_table(bufdb, "SELECT * FROM buffer", &table, &row, &col, NULL);
	if (ret != SQLITE_OK) {
		debug(DEBUG_WARNING, "could not get table: %s", sqlite3_errmsg(bufdb));
		return;
	}

	for (i = 0, j = col; i < row; i++, j += col) {
		uid = atoi(table[j]);
		snprintf(dbdata.client_name, sizeof(dbdata.client_name), "%s", table[j + 1]);
		snprintf(dbdata.client_ip, sizeof(dbdata.client_ip), "%s", table[j + 2]);
		snprintf(dbdata.sender_ip, sizeof(dbdata.sender_ip), "%s", table[j + 3]);
		dbdata.gps_tsp = atof(table[j + 4]);
		dbdata.gps_lat = atof(table[j + 5]);
		dbdata.gps_lon = atof(table[j + 6]);
		dbdata.packet_type = atoi(table[j + 7]);

		ret = db_insert(dbctx, &dbdata);
		if (!ret)
			break;
		ret = buffer_delete(uid);
		if (!ret)
			break;
	}

	sqlite3_free_table(table);
}

static void *buffer_routine(void *data)
{
	dbctx_t *ctx;
	struct timespec ts;
	int run = bufrun;

	debug(DEBUG_INFO, "buffer is started");
	while (run) {
		ctx = db_connect();
		if (ctx) {
			buffer_process(ctx);
			db_close(ctx);
		}
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec += config.buffer_interval;
		pthread_mutex_lock(&condmutex);
		pthread_cond_timedwait(&bufcond, &condmutex, &ts);
		pthread_mutex_unlock(&condmutex);

		pthread_mutex_lock(&bufmutex);
		run = bufrun;
		pthread_mutex_unlock(&bufmutex);
		if (!run) {
			ctx = db_connect();
			if (ctx) {
				buffer_process(ctx);
				db_close(ctx);
			}
			break;
		}
	}
	debug(DEBUG_INFO, "buffer is stopped");
	return NULL;
}

static int buffer_start(void)
{
	int ret;

	bufrun = 1;
	ret = pthread_create(&bufthread, NULL, &buffer_routine, NULL);
	if (ret) {
		debug(DEBUG_ERROR, "could not create buffer thread: %s",
		      strerror(errno));
		bufrun = 0;
		return 0;
	}
	return 1;
}

int buffer_init(void)
{
	int ret;
	const char *cmd;

	ret = sqlite3_open(config.buffer_file, &bufdb);
	if (ret != SQLITE_OK) {
		debug(DEBUG_ERROR, "could not open buffer file: %s", sqlite3_errmsg(bufdb));
		sqlite3_close(bufdb);
		return 0;
	}

	ret = sqlite3_exec(bufdb, "PRAGMA synchronous = 1", NULL, NULL, NULL);
	if (ret != SQLITE_OK) {
		debug(DEBUG_ERROR, "could not set buffer option: %s", sqlite3_errmsg(bufdb));
		sqlite3_close(bufdb);
		return 0;
	}

	/* Create buffer table */
	cmd = "CREATE TABLE IF NOT EXISTS buffer("
	      "uid INTEGER PRIMARY KEY,"
	      "client_name TEXT,"
	      "client_ip TEXT,"
              "sender_ip TEXT,"
              "gps_tsp REAL,"
	      "gps_lat REAL,"
	      "gps_lon REAL,"
	      "packet_type INTEGER)";
	ret = sqlite3_exec(bufdb, cmd, NULL, NULL, NULL);
	if (ret != SQLITE_OK) {
		debug(DEBUG_ERROR, "could not create table: %s", sqlite3_errmsg(bufdb));
		return 0;
	}

	/* Start buffer consumer and writer thread */
	ret = buffer_start();
	return ret;
}

int buffer_insert(const struct db_data *db)
{
	char *cmd;
	int ret;

	cmd = sqlite3_mprintf("INSERT INTO buffer VALUES(NULL,'%q','%q','%q',%f,%f,%f,%i)",
			      db->client_name, db->client_ip, db->sender_ip,
			      db->gps_tsp, db->gps_lat, db->gps_lon, db->packet_type);
	ret = sqlite3_exec(bufdb, cmd, NULL, NULL, NULL);
	sqlite3_free(cmd);
	if (ret != SQLITE_OK) {
		debug(DEBUG_ERROR, "could not insert buffer: %s", sqlite3_errmsg(bufdb));
		return 0;
	}
	return 1;
}

void buffer_stop(void)
{
	pthread_mutex_lock(&bufmutex);
	bufrun = 0;
	pthread_mutex_unlock(&bufmutex);
	pthread_mutex_lock(&condmutex);
	pthread_cond_signal(&bufcond);
	pthread_mutex_unlock(&condmutex);
	pthread_join(bufthread, NULL);
}
