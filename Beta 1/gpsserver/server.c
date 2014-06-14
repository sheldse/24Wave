#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "utils.h"
#include "config.h"
#include "database.h"
#include "msg.h"

#define EVENT_UNICAST   1
#define EVENT_BCAST     2
#define EVENT_MCAST     3
#define EVENT_ACK       4
#define EVENT_ONLINE    7
#define EVENT_OFFLINE   8
#define EVENT_TIMEOUT   9

struct client_state {
	int type;                 /* Socket type, TCP or UDP */
	int status;               /* CLIENT_ONLINE or CLIENT_OFFLINE */
	size_t ctl_rcvd;          /* Total control packet has been received */
	struct in_addr iaddr;     /* IPv4 address of client */
	struct tgr_msg tgr;       /* Triger msg */
	struct ctl_msg ctl;       /* Control msg */
	struct ack_msg ack;       /* ACK msg */
	struct timespec last_tgr; /* Last time of trigger msg was sent  */
	struct timespec last_ack; /* Last time of ack msg was received */
};

static struct client_state *cstate[FD_SETSIZE];
static dbctx_t *dbctx;
static int sock_ctl;

static void cstate_new(int sock)
{
	struct client_state *cs;

	cs = malloc(sizeof(struct client_state));
	if (!cs) {
		debug(DEBUG_ERROR, "out of memory");
		exit(1);
	}
	memset(cs, 0, sizeof(struct client_state));
	cs->status = CTL_CLIENT_OFFLINE;
	clock_gettime(CLOCK_MONOTONIC, &cs->last_tgr);
	clock_gettime(CLOCK_MONOTONIC, &cs->last_ack);
	cstate[sock] = cs;
}

static void cstate_remove(int sock)
{
	free(cstate[sock]);
	cstate[sock] = NULL;
}

static int cstate_scan(const char *name)
{
	int i;
	struct client_state *cs;

	for (i = 0; i < FD_SETSIZE; i++) {
		if ((cs = cstate[i]) != NULL)
			if (strcmp(cs->ctl.name, name) == 0)
				return i;
	}
	return -1;
}

static void set_nonblock(int sock)
{
	long flag;
	int ret;

	flag = fcntl(sock, F_GETFL);
	if (flag == -1) {
		debug(DEBUG_ERROR, "fcntl: %s", strerror(errno));
		exit(1);
	}
	if ((flag & O_NONBLOCK) != O_NONBLOCK) {
		flag |= O_NONBLOCK;
		ret = fcntl(sock, F_SETFL, flag);
		if (ret == -1) {
			debug(DEBUG_ERROR, "fcntl: %s", strerror(errno));
			exit(1);
		}
	}
}

static int setup_sockctl(void)
{
	int ret, opt;
	struct sockaddr_in saddr;

	sock_ctl = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_ctl == -1) {
		debug(DEBUG_ERROR, "unable to create CTL socket: %s", strerror(errno));
		return 0;
	}

	opt = 1;
	setsockopt(sock_ctl, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	set_nonblock(sock_ctl);

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_port = htons(config.control_port);
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Bind to all interface */

	ret = bind(sock_ctl, (struct sockaddr*) &saddr, sizeof(saddr));
	if (ret == -1) {
		debug(DEBUG_ERROR, "unable to bind on CTL socket: %s", strerror(errno));
		close(sock_ctl);
		return 0;
	}
	ret = listen(sock_ctl, 10);
	if (ret == -1) {
		debug(DEBUG_ERROR, "unable to listen on CTL socket: %s", strerror(errno));
		close(sock_ctl);
	}
	return 1;
}

static void create_ucastsock(const struct in_addr *iaddr,
			     const struct ctl_msg *ctl)
{
	struct client_state *cs;
	int s;

	s = socket(AF_INET, SOCK_DGRAM, 0);
	if (s == -1) {
		debug(DEBUG_ERROR, "socket: %s", strerror(errno));
		_exit(1);
	}
	set_nonblock(s);

	/* Allocate unicast socket state to monitor for ACK reply */
	cs = cstate[s];
	if (cs == NULL) {
		cstate_new(s);
		cs = cstate[s];
		cs->status = CTL_CLIENT_ONLINE;
		cs->type = SOCK_DGRAM;
		memcpy(&cs->iaddr, iaddr, sizeof(cs->iaddr));
		memcpy(&cs->ctl, ctl, sizeof(cs->ctl));
	}
}

static int read_ctlmsg(int sock)
{
	int ret, ucast_sock;
	char ip[INET_ADDRSTRLEN];
	struct ctl_msg ctl;
	struct client_state *cs = cstate[sock];

	inet_ntop(AF_INET, &cs->iaddr, ip, sizeof(ip));
	while (cs->ctl_rcvd < sizeof(ctl)) {
		ret = recv(sock, (char*) &ctl + cs->ctl_rcvd,
			   sizeof(ctl) - cs->ctl_rcvd, 0);
		if (ret == -1) {
			if (errno == EAGAIN)
				return 0;
			debug(DEBUG_ERROR, "recv was failed on addr=%s", ip);
			return -1;
		} else if (ret == 0) {
			debug(DEBUG_WARNING, "connection was closed addr=%s", ip);
			return -1;
		}
		cs->ctl_rcvd += ret;
	}
	msgctl_ntoh(&ctl);
	ret = msgctl_check(&ctl);
	if (ret != 0) {
		debug(DEBUG_WARNING, "invalid CTL msg hdr=%.4x ctl=%.4x addr=%s",
		      ctl.hdr, ctl.ctl, ip);
		return -1;
	}
	debug(DEBUG_INFO, "recvd CTL msg code=%s client='%s' addr=%s fd=%i", 
	      ctl.ctl == CTL_CLIENT_ONLINE ? "CLIENT_ONLINE" : "CLIENT_OFFLINE",
	      ctl.name, ip, sock);

	/* Scan if client is already online and sending CLIENT_ONLINE status */
	ucast_sock = cstate_scan(ctl.name);
	if (ucast_sock != -1 && ctl.ctl == CTL_CLIENT_ONLINE) {
		debug(DEBUG_WARNING, "client '%s' is already online", ctl.name);
		return -1;
	}

	if (ctl.ctl == CTL_CLIENT_OFFLINE) {
		/* Close UNICAST socket and remove client state */
		if (ucast_sock != -1) {
			cstate_remove(ucast_sock);
			close(ucast_sock);
		}
	} else { /* CTL_CLIENT_ONLINE) */
		cs->status = CTL_CLIENT_ONLINE;
		memcpy(&cs->ctl, &ctl, sizeof(ctl));
		if (config.unicast_enable)
			create_ucastsock(&cs->iaddr, &cs->ctl);
	}
	/* Write event to database */
	db_insertctl(dbctx, ctl.name, ip,
		     ctl.ctl == CTL_CLIENT_ONLINE ? EVENT_ONLINE : EVENT_OFFLINE);
	return 1;
}

/* Read unicast packet ACK */
static int read_ackmsg(int sock)
{
	int ret;
	unsigned addrlen;
	char ip[INET_ADDRSTRLEN];
	struct sockaddr_in addr;
	struct client_state *cs = cstate[sock];

	addrlen = sizeof(addr);
	ret = recvfrom(sock, &cs->ack, sizeof(cs->ack), 
		       0, (struct sockaddr*) &addr, &addrlen);
	if (ret == -1) {
		debug(DEBUG_ERROR, "recvfrom was failed: %s", strerror(errno));
		return 0;
	}

	inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
	msgack_ntoh(&cs->ack);
	ret = msgack_check(&cs->ack);
	if (ret != 0) {
		debug(DEBUG_WARNING, "invalid ACK msg hdr=%.4x crc=%.4x addr=%s fd=%i",
		      cs->ack.hdr, cs->ack.crc, ip, sock);
		return 0;
	}
	ret = sizeof(cs->ack.name);
	cs->ack.name[ret - 1] = 0;
	/* Save the last ack time */
	clock_gettime(CLOCK_MONOTONIC, &cs->last_ack);
	/* Write event to database */
	db_insertack(dbctx, &cs->ack, ip, EVENT_ACK);
	debug(DEBUG_INFO, "recvd ACK msg client='%s' lat=%s lon=%s tsp=%u addr=%s fd=%i", 
	      cs->ack.name, cs->ack.latitude, cs->ack.longitude, cs->ack.tsp, ip, sock);
	return 1;
}

static int send_packets(int sock,
			int type)
{
	struct sockaddr_in addr;
	struct tgr_msg msg;
	struct client_state *cs = cstate[sock];
	char ip[INET_ADDRSTRLEN];
	int s, ret, opt;

	/* Unicast */
	if (config.unicast_enable) {
		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port = config.clientport_enable ?
				htons(cs->ctl.uport) : htons(config.unicast_port);
		addr.sin_addr = cs->iaddr;

		msg.tsp = time(NULL);
		msgtgr_init(&msg);
		msgtgr_hton(&msg);
		ret = sendto(sock, &msg, sizeof(msg), 0, (struct sockaddr*) &addr, sizeof(addr));
		if (ret == -1) {
			debug(DEBUG_WARNING, "sendto: %s", strerror(errno));
			return 0;
		}
		inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
		debug(DEBUG_INFO, "sent TGR msg type=ucast client='%s' addr=%s fd=%i",
		      cs->ctl.name, ip, sock);
	}

	/* Multicast */
	if (config.multicast_enable) {
		s = socket(AF_INET, SOCK_DGRAM, 0);
		if (s == -1) {
			debug(DEBUG_ERROR, "socket: %s", strerror(errno));
			exit(1);
		}

		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port = config.clientport_enable ?
				htons(cs->ctl.mport) : htons(config.multicast_port);
		addr.sin_addr = cs->iaddr;

		msg.tsp = time(NULL);
		msgtgr_init(&msg);
		msgtgr_hton(&msg);
		ret = sendto(s, &msg, sizeof(msg), 0, (struct sockaddr*) &addr, sizeof(addr));
		if (ret == -1) {
			debug(DEBUG_WARNING, "sendto: %s", strerror(errno));
			close(s);
			return 0;
		}
		inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
		debug(DEBUG_INFO, "sent TGR msg type=mcast client='%s', addr=%s fd=%i",
		      cs->ctl.name, ip, sock);
		close(s);
	}

	/* Broadcast */
	if (config.broadcast_enable) {
		s = socket(AF_INET, SOCK_DGRAM, 0);
		if (s == -1) {
			debug(DEBUG_ERROR, "socket: %s", strerror(errno));
			exit(1);
		}

		opt = 1;
		setsockopt(s, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));

		memset(&addr, 0, sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port = config.clientport_enable ?
				htons(cs->ctl.bport) : htons(config.broadcast_port);
		addr.sin_addr = cs->iaddr;


		msg.tsp = time(NULL);
		msgtgr_init(&msg);
		msgtgr_hton(&msg);
		ret = sendto(s, &msg, sizeof(msg), 0, (struct sockaddr*) &addr, sizeof(addr));
		if (ret == -1) {
			debug(DEBUG_WARNING, "sendto: %s", strerror(errno));
			close(s);
			return 0;
		}
		inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
		debug(DEBUG_INFO, "sent TGR msg type=bcast client='%s', addr=%s fd=%i",
		      cs->ctl.name, ip, sock);
		close(s);
	}
	return 1;
}

static void timeout_ack(int sock,
			long diff)
{
	char ip[INET_ADDRSTRLEN];
	struct client_state *cs = cstate[sock];

	inet_ntop(AF_INET, &cs->iaddr, ip, sizeof(ip));
	db_insertctl(dbctx, cs->ctl.name, ip, EVENT_TIMEOUT);
	debug(DEBUG_INFO, "ACK msg is timeout client='%s' addr=%s fd=%i diff=%lims",
	      cs->ctl.name, ip, sock, diff);
	/* Close the UNICAST socket and deallocate it's state */
	close(sock);
	cstate_remove(sock);
}

static int handle_client(int sock)
{
	int ret;
	unsigned addrlen;
	char ip[INET_ADDRSTRLEN];
	struct sockaddr_in addr;
	struct client_state *cs;

	if (sock == sock_ctl) {
		addrlen = sizeof(addr);
		ret = accept(sock_ctl, (struct sockaddr*) &addr, &addrlen);
		if (ret == -1) {
			debug(DEBUG_WARNING, "accept was failed: %s", strerror(errno));
			return 0;
		}
		inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
		debug(DEBUG_INFO, "connection from addr=%s fd=%i", ip, ret);
		set_nonblock(ret);
		if (cstate[ret] == NULL) {
			cstate_new(ret);
			cstate[ret]->type = SOCK_STREAM;
			cstate[ret]->iaddr = addr.sin_addr;
		}
	} else {
		cs = cstate[sock];
		if (cs->type == SOCK_STREAM) {
			ret = read_ctlmsg(sock);
			/*
			 * Close client CTL socket once we have received CTL message to avoid
			 * run out of file descriptor, this socket is become unused
			 */
			if (ret != 0) {
				close(sock);
				cstate_remove(sock);
			}
		} else { /* UDP */
			read_ackmsg(sock);
		}
	}
	return 1;
}

static void accept_client(void)
{
	int ret, maxfd, i;
	long dtime;
	fd_set rset;
	struct client_state *cs;
	struct timeval tv;
	struct timespec ts;

	maxfd = sock_ctl;
	FD_ZERO(&rset);
	FD_SET(sock_ctl, &rset);
	for (i = 0; i < FD_SETSIZE; i++) {
		if (cstate[i] != NULL && i != sock_ctl) {
			FD_SET(i, &rset);
			if (i > maxfd)
				maxfd = i;
		}
	}

	tv = (struct timeval) { .tv_sec = 1, .tv_usec = 0L };
	ret = select(maxfd + 1, &rset, NULL, NULL, &tv);
	if (ret == -1) {
		debug(DEBUG_ERROR, "select was failed: %s", strerror(errno));
		return;
	}

	for (i = 0; i < FD_SETSIZE; i++) {
		if (FD_ISSET(i, &rset))
			handle_client(i);
		cs = cstate[i];
		if (cs != NULL && cs->type == SOCK_DGRAM && cs->status == CTL_CLIENT_ONLINE) {
			/* Send tgr message per packet interval */
			clock_gettime(CLOCK_MONOTONIC, &ts);
			dtime = tsdiff(&cs->last_tgr, &ts);
			if (dtime >= config.packet_interval) {
				send_packets(i, 0);
				/* Update last tgr send time */
				cs->last_tgr = ts;
			}
			/* Check for timeout ACK message */
			clock_gettime(CLOCK_MONOTONIC, &ts);
			dtime = tsdiff(&cs->last_ack, &ts);
			if (dtime >= config.prune_interval) {
				timeout_ack(i, dtime);
			}
		}
	}
}

int main(int argc,
	 char **argv)
{
	int ret, logfd, i;
	char *tmp, *progname;

	progname = argv[0];
	if ((tmp = strrchr(progname, '/')))
		progname = ++tmp;
	if (argc < 2) {
		fprintf(stderr, "Usage: %s <config-file>\n", progname);
		exit(1);
	}

	/* Read configuration */
	ret = config_read(argv[1]);
	if (!ret)
		exit(1);

	/* Open logfile */
	logfd = open(config.logfile_path, O_CREAT | O_APPEND | O_WRONLY, 0644);
	if (logfd == -1) {
		debug(DEBUG_ERROR, "unable to open logfile: %s", strerror(errno));
		exit(1);
	}

	/* Daemonize process */
	if (config.daemonize_enable) {
		ret = daemon(1, 1);
		if (ret == -1) {
			debug(DEBUG_ERROR, "unable to daemonize: %s", strerror(errno));
			exit(1);
		}
		debug(DEBUG_INFO, "started with pid %i", getpid());
		debug(DEBUG_INFO, "output are written in %s file", config.logfile_path);
		dup2(logfd, STDERR_FILENO);
		close(STDOUT_FILENO);
		close(STDIN_FILENO);
	}

	config_debug();

	/* Connect to database */
	dbctx = db_connect();
	if (!dbctx)
		exit(1);
	debug(DEBUG_INFO, "connected to database %s:%i", config.db_host, config.db_port);

	/* Setup control socket */
	ret = setup_sockctl();
	if (!ret)
		exit(1);
	debug(DEBUG_INFO, "control socket was created on 0.0.0.0:%i", config.control_port);

	/* Initialize client */
	for (i = 0; i < FD_SETSIZE; i++)
		cstate[i] = NULL;
	while (1) {
		accept_client();
	}

	exit(0);
}
