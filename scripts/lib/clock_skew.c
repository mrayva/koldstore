/* LD_PRELOAD shim for tests: shifts the wall clock (CLOCK_REALTIME, gettimeofday, time) of the
 * process tree by CLOCK_SKEW_SECONDS (may be negative). Monotonic clocks are untouched, like a
 * node whose NTP is off but whose kernel keeps ticking.
 *   gcc -shared -fPIC -o clockskew.so clock_skew.c -ldl */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

static long skew(void) {
    const char *s = getenv("CLOCK_SKEW_SECONDS");
    return s ? atol(s) : 0;
}

int clock_gettime(clockid_t clk, struct timespec *ts) {
    static int (*real)(clockid_t, struct timespec *);
    if (!real) real = dlsym(RTLD_NEXT, "clock_gettime");
    int rc = real(clk, ts);
    if (rc == 0 && (clk == CLOCK_REALTIME || clk == CLOCK_REALTIME_COARSE)) ts->tv_sec += skew();
    return rc;
}

int gettimeofday(struct timeval *tv, void *tz) {
    static int (*real)(struct timeval *, void *);
    if (!real) real = dlsym(RTLD_NEXT, "gettimeofday");
    int rc = real(tv, tz);
    if (rc == 0 && tv) tv->tv_sec += skew();
    return rc;
}

time_t time(time_t *out) {
    static time_t (*real)(time_t *);
    if (!real) real = dlsym(RTLD_NEXT, "time");
    time_t t = real(NULL) + skew();
    if (out) *out = t;
    return t;
}
