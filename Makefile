CC = gcc
CFLAGS = -std=c99 -Wall -Wextra -O2 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
LDFLAGS = -pthread

all: server client operations

server: src/server.c
	$(CC) $(CFLAGS) -o server src/server.c

client: src/client.c
	$(CC) $(CFLAGS) -o client src/client.c $(LDFLAGS)

operations: src/operations.c
	$(CC) $(CFLAGS) -o operations src/operations.c $(LDFLAGS)

clean:
	rm -f server client operations original.dat reassembled.dat result_min.txt result_max.txt result_sorted.dat execution_log.txt

test: all
	./run_test.sh
