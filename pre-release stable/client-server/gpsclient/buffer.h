#ifndef _BUFFER_H_
#define _BUFFER_H_

#include "database.h"

int buffer_init(void);
int buffer_insert(const struct db_data *db);
int buffer_stop(void);

#endif /* _BUFFER_H_ */
