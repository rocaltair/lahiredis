SLIB = liblahiredis.a
OBJS = lahiredis.o
CFILES = lahiredis.c
CC = gcc
AR = ar rvc 
#BACK_END : 0 for LAHIREDIS_BACKEND_LIBEV, 1 for LAHIREDIS_BACKEND_LIBEVENT
BACK_END = 0
RM = rm -f 

CFLAGS = -Wall -c -I. -ggdb -DLAHIREDIS_BACKEND=$(BACK_END)

all: $(SLIB)

$(OBJS): $(CFILES)

%.o: %.c
	$(CC) $(CFLAGS) $*.c

$(SLIB): $(OBJS)
	$(AR) $(SLIB) $(OBJS)

clean:
	$(RM) $(OBJS) $(SLIB)

.PHONY : clean

