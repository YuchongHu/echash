CC = gcc
CFLAGS = -std=c++11
LIBS = -lmemcached -lpthread

all: io scale repair

io: io.cpp
	$(CC) $(CFLAGS) io.cpp -o io $(LIBS)

scale: scale.cpp
	$(CC) $(CFLAGS) scale.cpp -o scale $(LIBS)

repair: repair.cpp
	$(CC) $(CFLAGS) repair.cpp -o repair $(LIBS)	

clean:
	rm io scale repair tmp.txt
