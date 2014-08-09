#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <gps.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "utils.h"
#include "msg.h"
#include "buffer.h"
#include "config.h"

static struct db_config dbcfg;
static struct gps_data_t gpsd;
static struct in_addr server_addr;
static pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;
static int ucast_sock;
static int mcast_sock;
static int bcast_sock;

static int read_gpsd(struct gps_fix_t *fix)
{
	int latlon_set;

	pthread_rwlock_rdlock(&rwlock);
	latlon_set = gpsd.set & LATLON_SET;
	memcpy(fix, &gpsd.fix, sizeof(struct gps_fix_t));
	pthread_rwlock_unlock(&rwlock);
	return (latlon_set && fix->mode > MODE_NO_FIX);
}

static void *gpsd_routine(void *data)
{
	int ret;

	while (1) {
		ret = gps_waiting(&gpsd, 1000);
		if (ret) {
			pthread_rwlock_wrlock(&rwlock);
			ret = gps_read(&gpsd);
			pthread_rwlock_unlock(&rwlock);
			if (ret == -1) {
				debug(DEBUG_ERROR, "unable to read gpsd: %s",
				      gps_errstr(errno));
				exit(1);
			}
		}
	}
	return NULL;
}

static void set_sockaddr(struct sockaddr_in *saddr,
			 const char *ipaddr,
			 unsigned short port)
{
	struct in_addr iaddr;
	int ret;

	memset(saddr, 0, sizeof(struct sockaddr_in));
	saddr->sin_family = AF_INET;
	saddr->sin_port = htons(port);
	if (strcmp(ipaddr, "0.0.0.0")) {
		ret = inet_pton(AF_INET, ipaddr, &iaddr);
		if (ret <= 0) {
			debug(DEBUG_WARNING, "invalid address %s", ipaddr);
			saddr->sin_addr.s_addr = htonl(INADDR_ANY);
		} else
			saddr->sin_addr = iaddr;
	} else
		saddr->sin_addr.s_addr = htonl(INADDR_ANY);
}

static int create_socket(int type,
			 const char *addr,
			 unsigned short port)
{
	int sock, ret, val;
	struct sockaddr_in saddr;
	struct in_addr iaddr;
	struct ip_mreq mreq;

	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock == -1)
		return sock;

	val = 1;
	ret = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int));
	if (ret == -1)
		return ret;

	/* Specify multicast group */
	if (type == CONFIG_MCAST) {
		ret = inet_pton(AF_INET, dbcfg.mcast_group, &iaddr);
		if (!ret) {
			debug(DEBUG_WARNING, "invalid mcast group addr %s", addr);
			return 0;
		}
		mreq.imr_multiaddr = iaddr;
		mreq.imr_interface.s_addr = htonl(INADDR_ANY);

		val = 1;
		ret = setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
		if (ret == -1)
			return ret;
	}

	set_sockaddr(&saddr, addr, port);
	ret = bind(sock, (struct sockaddr*) &saddr, sizeof(struct sockaddr_in));
	if (ret == -1)
		return -1;
	return sock;
}

static int prepare_sockets(void)
{
	ucast_sock = create_socket(CONFIG_UCAST, config.client_addr, dbcfg.ucast_port);
	if (ucast_sock == -1)
		return 0;
	mcast_sock = create_socket(CONFIG_MCAST, config.client_addr, dbcfg.mcast_port);
	if (mcast_sock == -1)
		return 0;
	bcast_sock = create_socket(CONFIG_BCAST, config.client_addr, dbcfg.bcast_port);
	if (bcast_sock == -1)
		return 0;
	return 1;
}

static void close_sockets(void)
{
	close(ucast_sock);
	close(mcast_sock);
	close(bcast_sock);
}

static void fill_db_data(const struct in_addr *addr,
			 const struct gps_fix_t *fix,
			 struct db_data *db,
			 int type)
{
	char ip_str[INET_ADDRSTRLEN];
	const char *ip_ptr;

	if (addr) {
		ip_ptr = inet_ntop(AF_INET, addr, ip_str, sizeof(ip_str));
		sprintf(db->sender_ip, "%s", ip_ptr);
	} else
		memset(db->sender_ip, 0, sizeof(db->sender_ip));

	if (type == CONFIG_UCAST)
		sprintf(db->client_ip, config.client_addr, sizeof(db->client_ip));
	else if (type == CONFIG_MCAST)
		sprintf(db->client_ip, config.client_addr, sizeof(db->client_ip));
	else if (type == CONFIG_BCAST)
		sprintf(db->client_ip, config.client_addr, sizeof(db->client_ip));
	else
		memset(db->client_ip, 0, sizeof(db->client_ip));

	sprintf(db->client_name, "%s", config.client_name);
	db->gps_tsp = fix->time;
	db->gps_lat = fix->latitude;
	db->gps_lon = fix->longitude;
	db->packet_type = type;
}

static int recv_msg(int sock,
		    struct sockaddr_in *saddr,
		    struct gps_fix_t *fix)
{
	struct db_data dbdata;
	struct tgr_msg msg;
	int ret, type;
	unsigned socklen;
	char ipstr[INET_ADDRSTRLEN];
	const char *str;

	if (sock == ucast_sock)
		str = "ucast";
	else if (sock == mcast_sock)
		str = "mcast";
	else
		str = "bcast";

	socklen = sizeof(struct sockaddr_in);
	ret = recvfrom(sock, &msg, sizeof(struct tgr_msg),
		       0, (struct sockaddr*) saddr, &socklen);
	if (ret == -1) {
		debug(DEBUG_WARNING, "recvfrom: %s", strerror(errno));
		return -1;
	} else if (ret != sizeof(msg)) {
		debug(DEBUG_WARNING, "invalid TGR msg length");
		return 0;
	}

	if (dbcfg.packet_validation) {
		msgtgr_ntoh(&msg);
		ret = msgtgr_check(&msg);
		if (ret != 0) {
			debug(DEBUG_WARNING, "invalid TGR msg type=%s hdr=%.4x crc=%.4x",
			      str, msg.hdr, msg.crc);
			return 0;
		}
	}

	inet_ntop(AF_INET, &saddr->sin_addr, ipstr, sizeof(ipstr));
	debug(DEBUG_INFO, "recvd TGR msg type=%s addr=%s fd=%i", str, ipstr, sock);

	ret = read_gpsd(fix);
	if (ret) {
		debug(DEBUG_INFO, "type=%s addr=%s tsp=%li lat=%f lon=%f", 
		      str, ipstr, (long) fix->time, fix->latitude, fix->longitude);
		if (isnan(fix->time) || isnan(fix->latitude) || isnan(fix->longitude)) {
			debug(DEBUG_WARNING, "invalid gps value (NAN)");
			return 0;
		}
		if (sock == ucast_sock)
			type = CONFIG_UCAST;
		else if (sock == mcast_sock)
			type = CONFIG_MCAST;
		else
			type = CONFIG_BCAST;
		fill_db_data(&saddr->sin_addr, fix, &dbdata, type);
		buffer_insert(&dbdata);
	} else {
		debug(DEBUG_WARNING, "no data from gpsd type=%s addr=%s", str, ipstr);
		return 0;
	}
	return 1;
}

static void *location_write(void *data)
{
	int ret;
	struct gps_fix_t fix;
	struct db_data db;

	for (;;) {
		ret = read_gpsd(&fix);
		if (ret) {
			fill_db_data(NULL, &fix, &db, CONFIG_MANUAL);
			buffer_insert(&db);
			debug(DEBUG_INFO, "location write tsp=%li lat=%f lon=%f", 
			      (long) fix.time, fix.latitude, fix.longitude);
		}
		msleep(dbcfg.location_writeival);
	}
	return NULL;
}

static int send_ctlmsg(int status)
{
	int sock, ret;
	size_t sent;
	struct sockaddr_in saddr;
	struct ctl_msg ctl;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) {
		debug(DEBUG_INFO, "socket: %s", strerror(errno));
		exit(1);
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(dbcfg.server_ctlport);
	saddr.sin_addr = server_addr;

	ret = connect(sock, (struct sockaddr*) &saddr, sizeof(saddr));
	if (ret == -1) {
		debug(DEBUG_ERROR, "connect: %s", strerror(errno));
		close(sock);
		return -1;
	}

	/* Initialize CTL msg to send */
	memcpy(ctl.name, dbcfg.name, sizeof(ctl.name));
	ctl.ctl = status;
	ctl.uport = dbcfg.ucast_port;
	ctl.mport = dbcfg.mcast_port;
	ctl.bport = dbcfg.bcast_port;
	msgctl_init(&ctl);
	msgctl_hton(&ctl);

	sent = 0;
	while (sent < sizeof(ctl)) {
		ret = send(sock, (char*) &ctl + sent, sizeof(ctl) - sent, 0);
		if (ret == -1) {
			debug(DEBUG_WARNING, "send: %s", strerror(errno));
			close(sock);
			return -1;
		}
		sent += ret;
	}
	debug(DEBUG_INFO, "sent CTL msg fd=%i status=%s", sock,
	      status == CTL_CLIENT_ONLINE ? "CLIENT_ONLINE" : "CLIENT_OFFLINE");
	return 1;
}

static int read_sockets(void)
{
	int ret, maxfd;
	fd_set rset;
	struct timeval tv;
	struct ack_msg ack;
	struct sockaddr_in saddr;
	struct gps_fix_t gfix;
	struct timespec ts1, ts2;

	maxfd = ucast_sock;
	if (mcast_sock > maxfd)
		maxfd = mcast_sock;
	if (bcast_sock > maxfd)
		maxfd = bcast_sock;

	/* Initialize last received TGR time */
	clock_gettime(CLOCK_MONOTONIC, &ts1);

	while (1) {
		FD_ZERO(&rset);
		FD_SET(ucast_sock, &rset);
		FD_SET(mcast_sock, &rset);
		FD_SET(bcast_sock, &rset);
		tv = (struct timeval) { .tv_sec = 1, .tv_usec = 0 };

		ret = select(maxfd + 1, &rset, NULL, NULL, &tv);
		if (ret == -1) {
			debug(DEBUG_ERROR, "select: %s", strerror(errno));
			return -1;
		} else if (ret == 0) { /* select was timeout */
			clock_gettime(CLOCK_MONOTONIC, &ts2);
			ret = tsdiff(&ts1, &ts2);
			if (ret >= dbcfg.server_retryival) {
				debug(DEBUG_INFO, "TGR msg recv was timeout");
				return 0;
			}
			continue;
		}

		/* Unicast */
		if (FD_ISSET(ucast_sock, &rset)) {
			ret = recv_msg(ucast_sock, &saddr, &gfix);
			if (ret == -1)
				return -1;
			if (ret) {
				/* Update last TGR recvd time */
				clock_gettime(CLOCK_MONOTONIC, &ts1);
				/* Reply with ACK msg */
				memcpy(ack.name, dbcfg.name, sizeof(ack.name));
				sprintf(ack.latitude, "%f", gfix.latitude);
				sprintf(ack.longitude, "%f", gfix.longitude);
				ack.tsp = gfix.time;
				msgack_init(&ack);
				msgack_hton(&ack);

				ret = sendto(ucast_sock, &ack, sizeof(ack), 
					     0, (struct sockaddr*) &saddr, sizeof(saddr));
				if (ret == -1) {
					debug(DEBUG_WARNING, "sendto: %s", strerror(errno));
					return -1;
				}
				debug(DEBUG_INFO, "sent ACK msg lat=%f lon=%f tsp=%li",
				      gfix.latitude, gfix.longitude, (long) gfix.time);
			}
		}
		/* Multicast */
		if (FD_ISSET(mcast_sock, &rset)) {
			ret = recv_msg(mcast_sock, &saddr, &gfix);
			if (ret == -1)
				return -1;
		}
		/* Broadcast */
		if (FD_ISSET(bcast_sock, &rset)) {
			ret = recv_msg(bcast_sock, &saddr, &gfix);
			if (ret == -1)
				return -1;
		}
	}
	return 1;
}

/* Get configuration from database */
int get_dbcfg(void)
{
	dbctx_t *dbctx;
	int ret;

	dbctx = db_connect();
	if (!dbctx)
		return -1;
	ret = db_getcfg(dbctx, &dbcfg);
	if (ret == -1) {
		debug(DEBUG_ERROR, "unable to read configuration from database");
		return -1;
	} else if (ret == 0) {
		debug(DEBUG_ERROR, "client name '%s' was not found in database", config.client_name);
		return 0;
	}
	db_close(dbctx);
	db_debugcfg(&dbcfg);
	return 1;
}

int get_serveraddr(void)
{
	struct hostent *he;
	char ip[INET_ADDRSTRLEN];

	he = gethostbyname(dbcfg.server_host);
	if (he == NULL) {
		debug(DEBUG_WARNING, "unable to resolve server host '%s'", dbcfg.server_host);
		return 0;
	}
	memcpy(&server_addr, he->h_addr_list[0], sizeof(struct in_addr));
	inet_ntop(AF_INET, &server_addr, ip, sizeof(ip));
	debug(DEBUG_INFO, "resolved %s as %s", dbcfg.server_host, ip);
	return 1;
}

static void signal_handler(int signo)
{
	struct ack_msg;

	if (signo == SIGTERM || signo == SIGINT) {
		debug(DEBUG_INFO, "got TERM or INT signal, sending OFFLINE status");
		send_ctlmsg(CTL_CLIENT_OFFLINE);
		debug(DEBUG_INFO, "processing buffer records");
		buffer_stop();
		exit(0);
	}
}

int main(int argc,
	 char **argv)
{
	pthread_t gpsd_thread;
	pthread_t writeloc_thread;
	struct timespec ts1, ts2;
	struct sigaction sa;
	char gpsd_port[5];
	int ret, nretry;
	char *progname, *tmp;

	progname = argv[0];
	if ((tmp = strstr(argv[0], "/")))
		progname = ++tmp;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <config-file>\n", progname);
		exit(EXIT_SUCCESS);
	}

	ret = config_read(argv[1]);
	if (!ret) {
		debug(DEBUG_ERROR, "unable to read config file");
		exit(EXIT_FAILURE);
	}

	/* Initialize GPSD connection */
	sprintf(gpsd_port, "%i", config.gpsd_port);
	ret = gps_open(config.gpsd_addr, gpsd_port, &gpsd);
	if (ret == -1) {
		debug(DEBUG_ERROR, "unable to connect to gpsd");
		exit(EXIT_FAILURE);
	}

	/* Enable GPSD streaming */
	ret = gps_stream(&gpsd, WATCH_ENABLE, NULL);
	if (ret == -1) {
		debug(DEBUG_ERROR, "unable to enable gpsd streaming: %s", gps_errstr(errno));
		exit(1);
	}
	debug(DEBUG_INFO, "gpsd streams enabled %s:%i", config.gpsd_addr, config.gpsd_port);

	/* Start gpsd routine */
	ret = pthread_create(&gpsd_thread, NULL, gpsd_routine, NULL);
	if (ret) {
		debug(DEBUG_ERROR, "unable to create gpsd routine\n");
		exit(1);
	}

	/* Initialize buffer */
	ret = buffer_init();
	if (!ret) {
		debug(DEBUG_ERROR, "unable to initialize buffer file");
		exit(EXIT_FAILURE);
	}

	/* Get configuration from database */
	nretry = 0;
	while (1) {
		nretry++;
		debug(DEBUG_INFO, "reading config from database try=%i", nretry);
		if (get_dbcfg() > 0 && get_serveraddr())
			break;
		msleep(30000);
		if (nretry >= 5) {
			debug(DEBUG_ERROR, "timeout reading config from database");
			exit(1);
		}
	}

	/* Set last time of config read */
	clock_gettime(CLOCK_MONOTONIC, &ts1);

	/* Start local location write routine */
	ret = pthread_create(&writeloc_thread, NULL, location_write, NULL);
	if (ret) {
		debug(DEBUG_ERROR, "unable to create location write routine");
		exit(1);
	}

	/* Install signal handler */
	sa.sa_handler = signal_handler;
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);

	/* Main loop */
	while (1) {
		clock_gettime(CLOCK_MONOTONIC, &ts2);
		if (tsdiff(&ts1, &ts2) >= 5000) {
			debug(DEBUG_INFO, "attemping to reread config from database");
			ret = get_dbcfg();
			if (ret <= 0) {
				msleep(dbcfg.server_retryival);
				continue;
			}
			ts1 = ts2;
		}
		ret = send_ctlmsg(CTL_CLIENT_ONLINE);
		if (ret <= 0) {
			msleep(dbcfg.server_retryival);
			continue;
		}

		if (prepare_sockets() == -1)
			exit(1);

		ret = read_sockets();
		if (ret == -1)
			exit(1);
		close_sockets();
	}

	/* Not reached */
	gps_close(&gpsd);
	pthread_join(gpsd_thread, NULL);
	pthread_join(writeloc_thread, NULL);
	exit(0);
}
