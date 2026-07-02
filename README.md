# CNS6214 OS Project

This is a simple Linux-focused reference version of the full assignment workflow.

Target environment:

```text
Linux x86_64, Ubuntu 22.04+ recommended
GCC
POSIX sockets, fork, pthreads, POSIX semaphores
```

## Member A Scope

- `src/server.c`
  - Starts a TCP server on port 9090.
  - Accepts client connections.
  - Uses `fork()` so each child process handles one chunk request.
  - Receives `total_chunks` and `chunk_number` from the client.
  - Sends an 8-byte header followed by the requested file chunk.

- `src/client.c`
  - Runs as `./client -p <N>`.
  - Requests the file size from the server.
  - Forks `N` child processes.
  - Each child receives one chunk.
  - Uses shared memory for the reassembly buffer.
  - Uses mutex, semaphore, and condition variable for synchronization.
  - Writes `reassembled.dat`.
  - Automatically launches `./operations -t 4 -f reassembled.dat`.

- `src/operations.c`
  - Implements parallel MIN and MAX calculation.
  - Implements a simple parallel merge sort.
  - Uses `pthread_create()` and `pthread_join()`.
  - Uses mutex, semaphore, and condition variable.
  - Writes `result_min.txt`, `result_max.txt`, and `result_sorted.dat`.

## Protocol Note

The assignment requires each chunk to be sent as:

```text
4 bytes: sequence number
4 bytes: payload size
payload data
```

This implementation follows that format for chunk transfer.

Before chunk transfer, the client sends one simple metadata request:

```text
total_chunks = 0
chunk_number = 0
```

The server replies with the file size. This allows the client to allocate the
correct reassembly buffer before forking child processes.

## Simple Build

```bash
make
```

On Linux, the programs build into:

```text
server
client
operations
```

## Run Server

```bash
./server original.dat
```

## Run Client

```bash
./client -p 4
```

## Run Full Test

```bash
./run_test.sh
```

The test creates a small binary file, runs server/client/operations, and checks:

- `original.dat` matches `reassembled.dat`
- `result_min.txt` is correct
- `result_max.txt` is correct
- `result_sorted.dat` is sorted correctly
- `execution_log.txt` contains the required markers
