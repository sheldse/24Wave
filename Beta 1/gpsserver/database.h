#ifndef _DATABASE_H_
#define _DATABASE_H_

#include <libpq-fe.h>
#include "msg.h"

typedef PGconn dbctx_t;

dbctx_t *db_connect(void);

void db_close(dbctx_t *ctx);

int db_insertctl(dbctx_t *ctx,
		 const char *name,
		 const char *addr, 
		 int event);

int db_insertack(dbctx_t *ctx,
                 const struct ack_msg *ack,
                 const char *addr,
                 int event);

#endif /* _DATABASE_H_ */
