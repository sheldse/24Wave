#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "config.h"
#include "utils.h"

static const char * const config_keys[] = { 
	"client-name",
	"client-addr",
	"multicast-group-addr",
	"gpsd-addr",
	"gpsd-port",
	"db-addr",
	"db-port",
	"db-name",
	"db-user",
	"db-passwd",
	"db-tablecfg",
	"db-tabledata",
	"buffer-file",
	"buffer-interval",
	NULL
};

static void xstrncpy(char *dest, const char *src, int size)
{
	int len;
	if (strlen(src) >= size)
		len = size - 1;
	else
		len = strlen(src);
	memcpy(dest, src, len);
	dest[len] = 0;
}

static void config_debug(void)
{
	debug(DEBUG_INFO, "client-name=%s", config.client_name);
	debug(DEBUG_INFO, "client-addr=%s  multicast-group-addr=%s",
	      config.client_addr, config.mcast_gaddr);
	debug(DEBUG_INFO, "gpsd-addr=%s gpsd-port=%i", config.gpsd_addr, config.gpsd_port);
	debug(DEBUG_INFO, "db-addr=%s db-port=%i db-name=%s db-user=%s db-passwd=%s "
	      "db-tablecfg=%s db-tabledata=%s", config.db_addr, config.db_port, config.db_name, 
	      config.db_user, config.db_passwd, config.db_tablecfg, config.db_tabledata);
	debug(DEBUG_INFO, "buffer-file=%s buffer-interval=%i", config.buffer_file,
	      config.buffer_interval);
}

const char *config_get_value(char *line)
{
	/* line is always null terminated */
	char *p = line;
	char *t;

	/* Skip key */
	while (*p != ' ' && *p != '\t')
		if (*p++ == '\0')
			return NULL;
	/* Skip spaces or tabs */
	while (*p == ' ' || *p == '\t')
		if (*p++ == '\0')
			return NULL;
	/* Replace '\n' with '\0' */
	t = p;
	while (*t) {
		if (*t == '\n') {
			*t = '\0';
			break;
		}
		t++;
	}
	return p;
}

static void config_set_value(const char *value, int id)
{
	switch (id) {
		case 0: /* client-name */
			xstrncpy(config.client_name, value, sizeof(config.client_name));
			break;
		case 1: /* client-addr */
			xstrncpy(config.client_addr, value, sizeof(config.client_addr));
			break;
		case 2: /* multicast-group-addr */
			xstrncpy(config.mcast_gaddr, value, sizeof(config.mcast_gaddr));
			break;
		case 3: /* gpsd-addr */
			xstrncpy(config.gpsd_addr, value, sizeof(config.gpsd_addr));
			break;
		case 4: /* gpsd-port */
			config.gpsd_port = atoi(value);
			break;
		case 5: /* db-addr */
			xstrncpy(config.db_addr, value, sizeof(config.db_addr));
			break;
		case 6: /* db-port */
			config.db_port = atoi(value);
			break;
		case 7: /* db-name */
			xstrncpy(config.db_name, value, sizeof(config.db_name));
			break;
		case 8: /* db-user */
			xstrncpy(config.db_user, value, sizeof(config.db_user));
			break;
		case 9: /* db-passwd */
			xstrncpy(config.db_passwd, value, sizeof(config.db_passwd));
			break;
		case 10: /* db-tablecfg */
			xstrncpy(config.db_tablecfg, value, sizeof(config.db_tablecfg));
			break;
		case 11: /* db-tabledata */
			xstrncpy(config.db_tabledata, value, sizeof(config.db_tabledata));
			break;
		case 12: /* buffer-file */
			xstrncpy(config.buffer_file, value, sizeof(config.buffer_file));
			break;
		case 13: /* buffer-interval */
			config.buffer_interval = atoi(value);
			if (config.buffer_interval <= 0)
				config.buffer_interval = 10;
			break;
	}
}

static void config_default(void)
{
	/* Connections */
	sprintf(config.client_name, "%s", "client-name");
	sprintf(config.client_addr, "%s", "0.0.0.0");
	sprintf(config.mcast_gaddr, "%s", "224.0.0.1");

	/* GPSD */
	sprintf(config.gpsd_addr, "%s", "127.0.0.1");
        config.gpsd_port = 2947;

	/* PostgreSQL */
	sprintf(config.db_addr, "%s", "127.0.0.1");
        config.db_port = 5432;
        sprintf(config.db_name, "%s", "db-name");
        sprintf(config.db_user, "%s", "db-user");
        sprintf(config.db_passwd, "%s", "db-passwd");
        sprintf(config.db_tablecfg, "%s", "dbtablecfg");
        sprintf(config.db_tabledata, "%s", "dbtabledata");

	/* Buffer */
	sprintf(config.buffer_file, "%s", "/tmp/gpsclient.db");
	config.buffer_interval = 10;
}

int config_read(const char *file)
{
	FILE *fp;
	char buffer[BUFSIZ];
	const char *value;
	int i;

	fp = fopen(file, "r");
	if (!fp) {
		debug(DEBUG_ERROR, "%s: %s", file, strerror(errno));
		return 0;
	}
	config_default();
	while (fgets(buffer, BUFSIZ, fp)) {
		if (*buffer == '#') /* Comments */
			continue;
		for (i = 0; config_keys[i] != NULL; i++)
			if (!strncmp(config_keys[i], buffer, strlen(config_keys[i]))) {
				value = config_get_value(buffer);
				if (value)
					config_set_value(value, i);
				break;
			}
	}
	fclose(fp);
	config_debug();
	return 1;
}
