SLIB = liblahiredis.a
OBJS = lahiredis.o
CFILES = lahiredis.c
CC = gcc
AR = ar rvc 
#BACK_END : 0 for LAHIREDIS_BACKEND_LIBEV, 1 for LAHIREDIS_BACKEND_LIBEVENT
BACK_END = libevent
BACK_END_NUM = 1
RM = rm -f 

ifeq ($(BACK_END), libev)
	BACK_END_NUM = 0
endif

CFLAGS = -Wall -c -I. -ggdb -DLAHIREDIS_BACKEND=$(BACK_END_NUM)

all: $(SLIB)

libev :
	$(MAKE) BACK_END=libev

libevent :
	$(MAKE) BACK_END=libevent

$(OBJS): $(CFILES)

%.o: %.c
	$(CC) $(CFLAGS) $*.c

$(SLIB): $(OBJS)
	$(AR) $(SLIB) $(OBJS)

clean:
	$(RM) $(OBJS) $(SLIB)

.PHONY : clean libev libevent

