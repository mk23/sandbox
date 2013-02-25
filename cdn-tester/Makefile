SRC = dns_hijack.c
DST = libdns_hijack.so.1
LIBS = -ldl

CC  ?= gcc
CFLAGS += -fPIC
LDFLAGS += -rdynamic $(LIBS)

all:	$(DST)

$(DST):	$(SRC)
	$(CC) $(CFLAGS) -o $(DST) $(SRC) -shared -Wl,-soname,$(DST) $(LDFLAGS)
