#include <arpa/inet.h>
#include "crc16.h"
#include "msg.h"

void msgtgr_init(struct tgr_msg *msg)
{
	msg->hdr = TGR_MSG_HDR;
	msg->crc = crc16(0, (char*) msg + 4, sizeof(*msg) - 4);
}

int msgtgr_check(const struct tgr_msg *msg)
{
	int crc;

	if (msg->hdr != TGR_MSG_HDR)
		return -1;
	crc = crc16(0, (char*) msg + 4, sizeof(*msg) - 4);
	if (crc != msg->crc)
		return -2;
	return 0;
}

void msgtgr_ntoh(struct tgr_msg *msg)
{
	msg->hdr = ntohs(msg->hdr);
	msg->crc = ntohs(msg->crc);
	msg->tsp = ntohl(msg->tsp);
}

void msgtgr_hton(struct tgr_msg *msg)
{
	msg->hdr = htons(msg->hdr);
	msg->crc = htons(msg->crc);
	msg->tsp = htonl(msg->tsp);
}

void msgctl_init(struct ctl_msg *msg)
{
	msg->hdr = CTL_MSG_HDR;
}

int msgctl_check(const struct ctl_msg *msg)
{
	if (msg->hdr != CTL_MSG_HDR)
		return -1;
	if (msg->ctl != CTL_CLIENT_ONLINE && msg->ctl != CTL_CLIENT_OFFLINE)
		return -2;
	return 0;
}

void msgctl_ntoh(struct ctl_msg *msg)
{
	msg->hdr = ntohs(msg->hdr);
	msg->ctl = ntohs(msg->ctl);
	msg->uport = ntohs(msg->uport);
	msg->mport = ntohs(msg->mport);
	msg->bport = ntohs(msg->bport);
}

void msgctl_hton(struct ctl_msg *msg)
{
	msg->hdr = htons(msg->hdr);
	msg->ctl = htons(msg->ctl);
	msg->uport = htons(msg->uport);
	msg->mport = htons(msg->mport);
	msg->bport = htons(msg->bport);
}

void msgack_init(struct ack_msg *msg)
{
	msg->hdr = ACK_MSG_HDR;
	msg->crc = crc16(0, (char*) msg + 4, sizeof(*msg) - 4);
}

int msgack_check(const struct ack_msg *msg)
{
	int crc;

	if (msg->hdr != ACK_MSG_HDR)
		return -1;
	crc = crc16(0, (char*) msg + 4, sizeof(*msg) - 4);
	if (crc != msg->crc)
		return -2;
	return 0;
}

void msgack_ntoh(struct ack_msg *msg)
{
	msg->hdr = ntohs(msg->hdr);
	msg->crc = ntohs(msg->crc);
	msg->tsp = ntohl(msg->tsp);
}

void msgack_hton(struct ack_msg *msg)
{
	msg->hdr = htons(msg->hdr);
	msg->crc = htons(msg->crc);
	msg->tsp = htonl(msg->tsp);
}
