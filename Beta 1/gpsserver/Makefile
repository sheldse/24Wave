# gpsserver Makefile

SOURCES  = utils.c crc16.c msg.c config.c database.c server.c
OBJECTS  = ${SOURCES:.c=.o}
CFLAGS   = -Wall -g -fstack-protector -I/usr/include/postgresql -I../libs
LDFLAGS  = -lrt -lpthread -lpq
TARGET   = gpsserver

${TARGET}: ${OBJECTS} ${LIBS_OBJ}
	${CC} ${OBJECTS} ${LDFLAGS} -o ${TARGET}

msg.o: ../libs/msg.c
	${CC} ${CFLAGS} -c $<

utils.o: ../libs/utils.c
	${CC} ${CFLAGS} -c $<

crc16.o: ../libs/crc16.c
	${CC} ${CFLAGS} -c $<

.c.o:
	${CC} ${CFLAGS} -c $<

clean:
	rm -rf *.o ${TARGET}

