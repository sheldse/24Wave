#ifndef _MSG_H_
#define _MSG_H_

#define TGR_MSG_HDR  0xa0f9

struct tgr_msg {
        unsigned short hdr;     /* Header */
        unsigned short crc;     /* CRC16 */
        unsigned int tsp;       /* Timestamp */
	char __reserved[1016];	/* Reserved */
};

void msgtgr_init(struct tgr_msg *msg);
int msgtgr_check(const struct tgr_msg *msg);
void msgtgr_ntoh(struct tgr_msg *msg);
void msgtgr_hton(struct tgr_msg *msg);

#define CTL_MSG_HDR  0xa1f9
#define CTL_CLIENT_ONLINE   0x0001
#define CTL_CLIENT_OFFLINE  0x0002

struct ctl_msg {
	unsigned short hdr;	/* Header */
	unsigned short ctl;	/* Control code */
	unsigned short uport;	/* Unicast port */
	unsigned short mport;	/* Multicast port */
	unsigned short bport;	/* Broadcast port */
	unsigned short __pad1;	/* Unused */
	char name[16];		/* Client name */
};

void msgctl_init(struct ctl_msg *msg);
int msgctl_check(const struct ctl_msg *msg);
void msgctl_ntoh(struct ctl_msg *msg);
void msgctl_hton(struct ctl_msg *msg);

#define ACK_MSG_HDR  0xa2f9

struct ack_msg {
	unsigned short hdr;	/* Header */
	unsigned short crc;	/* CRC16 */
	char name[16];		/* Sender name */
	char latitude[16];	/* GPS Latitude */
	char longitude[16];	/* GPS Longitude */
	unsigned int tsp;	/* GPS timestamp */
};

void msgack_init(struct ack_msg *msg);
int msgack_check(const struct ack_msg *msg);
void msgack_ntoh(struct ack_msg *msg);
void msgack_hton(struct ack_msg *msg);

#endif
