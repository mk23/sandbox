#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

static struct hostent * (*__gethostbyname)(const char *) = NULL;

struct hostent * gethostbyname(const char *name) {
    struct in_addr a;
    struct hostent * h = NULL;
    const char * hijack_host = getenv("HIJACK_HOST");
    const char * hijack_addr = getenv("HIJACK_ADDR");

    openlog("dns_hijack", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL7);

    if (NULL == __gethostbyname) {
        __gethostbyname = (struct hostent * (*)(const char *))dlsym(RTLD_NEXT, "gethostbyname");
        if (NULL != dlerror()) {
            syslog(LOG_ERR, "failed to look up gethostbyname()");
            return NULL;
        }
    }

    if (NULL != (h = __gethostbyname(name))) {
        if (NULL != hijack_host && NULL != hijack_addr 
                && strncasecmp(name, hijack_host, strlen(name)) == 0
                && inet_aton(hijack_addr, &a) != 0) {
            syslog(LOG_INFO, "hijacking gethostbyname for %s from %s to %s", name, inet_ntoa(*(struct in_addr *)h->h_addr), hijack_addr);
            ((struct in_addr *)h->h_addr)->s_addr = a.s_addr;
        }
    }

    closelog();

    return h;
}
