#include <time.h>
#include <errno.h>

/* Suspend thread in miliseconds precision */
void msleep(int ms)
{
	struct timespec req, rem;

	req.tv_sec = ms / 1000;
	req.tv_nsec = ms % 1000 * 1000000;

	while (nanosleep(&req, &rem) == -1 && errno == EINTR)
		req = rem;
}

/* Return time difference between given timespecs in miliseconds */
long tsdiff(const struct timespec *ts1,
	    const struct timespec *ts2)
{
	long ms1, ms2;

	ms1 = (ts1->tv_sec * 1000) + (ts1->tv_nsec / 1000000);
	ms2 = (ts2->tv_sec * 1000) + (ts2->tv_nsec / 1000000);
	return ms2 - ms1;
}
