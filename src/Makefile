SLIB=liblahiredis.a
OFILES= lahiredis.o
CC = gcc
AR = ar
CFLAGS = -Wall -c -I. -ggdb

#BACK_END : 0 for LAHIREDIS_BACKEND_LIBEV, 1 for LAHIREDIS_BACKEND_LIBEVENT
BACK_END = 0
RM = rm -f 

all: $(SLIB)

$(OFILES): lahiredis.c

%.o: %.c
	$(CC) -DLAHIREDIS_BACKEND=$(BACK_END) $(CFLAGS) $*.c

$(SLIB): $(OFILES)
	$(AR) rvc $(SLIB) $(OFILES)

clean:
	$(RM) *.o $(SLIB)

.PHONY : clean

