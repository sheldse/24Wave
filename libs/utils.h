#ifndef _UTILS_H_
#define _UTILS_H_

#include <sys/time.h>
#include <time.h>
#include <stdio.h>

enum {
	DEBUG_FATAL,
	DEBUG_ERROR,
	DEBUG_WARNING,
	DEBUG_INFO
};

#define debug(level, ...) \
do { \
	const char * const __debug_string[4] = { "FATL", "ERRR", "WARN", "INFO" }; \
	char __fmt1[2048]; \
	char __fmt2[2048+128]; \
	struct timeval __tv; \
	struct tm __st; \
	gettimeofday(&__tv, NULL); \
	localtime_r(&__tv.tv_sec, &__st); \
	snprintf(__fmt1, sizeof(__fmt1), __VA_ARGS__); \
	snprintf(__fmt2, sizeof(__fmt2), "[%s] %i%.2i%.2i.%.2i%.2i%.2i.%.2i %s [%s:%i]\n", \
		 __debug_string[level], (__st.tm_year + 1900) % 100, __st.tm_mon + 1, __st.tm_mday, \
		 __st.tm_hour, __st.tm_min, __st.tm_sec, (int) __tv.tv_usec / 10000, __fmt1, \
		 __FILE__, __LINE__); \
	fprintf(stderr, "%s", __fmt2); \
} while (0)

void msleep(int ms);
long tsdiff(const struct timespec *ts1,
	    const struct timespec *ts2);

#endif /* _UTILS_H_ */
