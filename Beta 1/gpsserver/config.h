#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <netinet/in.h>

#define CONFIG_MANUAL 0
#define CONFIG_UCAST  1
#define CONFIG_MCAST  2
#define CONFIG_BCAST  3

struct config {
	unsigned short control_port;
	int unicast_enable;
	unsigned short unicast_port;
	int broadcast_enable;
	unsigned short broadcast_port;
	int multicast_enable;
	unsigned short multicast_port;
	int clientport_enable;
	int packet_interval;
	int prune_interval;
	char db_host[16];
	unsigned short db_port;
	char db_name[16];
	char db_user[16];
	char db_passwd[16];
	char db_table[32];
	char logfile_path[128];
	char pidfile_path[128];
	int daemonize_enable;
};

/* Globally accessed configuration */
struct config config;

int config_read(const char *file);
void config_debug(void);

#endif /* _CONFIG_H_ */
