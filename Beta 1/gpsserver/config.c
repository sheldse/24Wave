#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "config.h"
#include "utils.h"

static const char * const config_keys[] = {
	"control-port",
	"unicast-enable",
	"unicast-port",
	"broadcast-enable",
	"broadcast-port",
	"multicast-enable",
	"multicast-port",
	"clientport-enable",
	"packet-interval",
	"prune-interval",
	"db-host",
	"db-port",
	"db-name",
	"db-user",
	"db-passwd",
	"db-table",
	"logfile-path",
	"pidfile-path",
	"daemonize-enable",
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

void config_debug(void)
{
	debug(DEBUG_INFO, "control-port=%i", config.control_port);
	debug(DEBUG_INFO, "unicast-enable=%s unicast-port=%i", 
	      config.unicast_enable ? "yes" : "no", config.unicast_port);
	debug(DEBUG_INFO, "multicast-enable=%s multicast-port=%i", 
	      config.multicast_enable ? "yes" : "no", config.multicast_port);
	debug(DEBUG_INFO, "broadcast-enable=%s broadcast-port=%i", 
	      config.broadcast_enable ? "yes" : "no", config.broadcast_port);
	debug(DEBUG_INFO, "clientport-enable=%s", config.clientport_enable ? "yes" : "no");
	debug(DEBUG_INFO, "packet-interval=%ims", config.packet_interval);
	debug(DEBUG_INFO, "prune-interval=%ims", config.prune_interval);
	debug(DEBUG_INFO, "db-host=%s db-port=%i db-name=%s db-user=%s db-passwd=%s db-table=%s",
	      config.db_host, config.db_port, config.db_name, 
	      config.db_user, config.db_passwd, config.db_table);
	debug(DEBUG_INFO, "logfile-path=%s", config.logfile_path);
	debug(DEBUG_INFO, "pidfile-path=%s", config.pidfile_path);
	debug(DEBUG_INFO, "daemonize-enable=%s", config.daemonize_enable ? "yes" : "no");
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
		case 0: /* control-port */
			config.control_port = atoi(value); 
			break;
		case 1: /* unicast-enable */
			config.unicast_enable = strcmp("yes", value) ? 0 : 1;
			break;
		case 2: /* unicast-port */
			config.unicast_port = atoi(value);
			break;
		case 3: /* broadcast-enable */
			config.broadcast_enable = strcmp("yes", value) ? 0 : 1;
			break;
		case 4: /* broadcast-port */
			config.broadcast_port = atoi(value);
			break;
		case 5: /* multicast-enable */
			config.multicast_enable = strcmp("yes", value) ? 0 : 1;
			break;
		case 6: /* multicast-port */
			config.multicast_port = atoi(value);
			break;
		case 7: /* clientport-enable */
			config.clientport_enable = strcmp("yes", value) ? 0 : 1;
			break;
		case 8: /* packet-interval */
			config.packet_interval = atoi(value);
			break;
		case 9: /* prune-interval */
			config.prune_interval = atoi(value);
			break;
		case 10: /* db-host */
			xstrncpy(config.db_host, value, sizeof(config.db_host));
			break;
		case 11: /* db-port */
			config.db_port = atoi(value);
			break;
		case 12: /* db-name */
			xstrncpy(config.db_name, value, sizeof(config.db_name));
			break;
		case 13: /* db-user */
			xstrncpy(config.db_user, value, sizeof(config.db_user));
			break;
		case 14: /* db-passwd */
			xstrncpy(config.db_passwd, value, sizeof(config.db_passwd));
			break;
		case 15: /* db-table */
			xstrncpy(config.db_table, value, sizeof(config.db_table));
			break;
		case 16: /* logfile-path */
			xstrncpy(config.logfile_path, value, sizeof(config.logfile_path));
			break;
		case 17: /* pidfile-path */
			xstrncpy(config.pidfile_path, value, sizeof(config.pidfile_path));
			break;
		case 18: /* daemonize-enable */
			config.daemonize_enable = strcmp("yes", value) ? 0 : 1;
			break;
	}
}

static void config_default(void)
{
	/* Connections */
	config.control_port = 5000;
	config.unicast_enable = 1;
        config.unicast_port = 6000;
        config.multicast_enable = 1;
	config.multicast_port = 6001;
	config.broadcast_enable = 1;
	config.broadcast_port = 6002;
	config.clientport_enable = 0;
	config.packet_interval = 5000;
	config.prune_interval = 5000;

	/* Database */
	sprintf(config.db_host, "%s", "127.0.0.1");
        config.db_port = 5432;
        sprintf(config.db_name, "%s", "db-name");
        sprintf(config.db_user, "%s", "db-user");
        sprintf(config.db_passwd, "%s", "db-passwd");
	sprintf(config.db_table, "%s", "db-table");

	/* Misc */
	sprintf(config.logfile_path, "%s", "/tmp/gpsserver.log");
	sprintf(config.pidfile_path, "%s", "/tmp/gpsserver.pid");
	config.daemonize_enable = 0;
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
	return 1;
}
