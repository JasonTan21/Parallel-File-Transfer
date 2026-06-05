# Member A Notes

## What I Am Responsible For

My main responsibility is:

1. Server / producer side file transfer.
2. Parallel MIN calculation in `operations.c`.
3. File integrity validation using `md5sum`.

This folder now also contains a complete simple Linux reference version for the
client and analytics parts so I can understand how the whole assignment works
before merging with group members.

## Server Flow

The server runs with:

```bash
./server <input_file>
```

Simple explanation:

1. Check that the user gave an input file.
2. Create a TCP socket using `socket()`.
3. Bind the socket to port `9090` using `bind()`.
4. Start listening using `listen()`.
5. Accept client connections using `accept()`.
6. For each connection, call `fork()`.
7. The child process receives the requested chunk number.
8. The child reads that chunk from the file.
9. The child sends:
   - sequence number
   - payload size
   - payload data
10. The child exits.

## Server Request Format

The client sends 8 bytes:

```text
4 bytes: total number of chunks
4 bytes: requested chunk number
```

Both numbers are sent in network byte order.

## Server Response Format

The server sends the format required by the assignment:

```text
4 bytes: sequence number
4 bytes: payload size
payload: raw int32_t file data
```

## Chunk Calculation

The server calculates chunk size using:

```c
chunk_size = (file_size + total_chunks - 1) / total_chunks;
```

This rounds up, so every chunk except possibly the last chunk has the same size.

The file offset for a chunk is:

```c
offset = (chunk_number - 1) * chunk_size;
```

## Why `send_all()` Is Needed

`send()` may send only part of the data.

So `send_all()` loops until all bytes are sent. This is important for large files because a 1GB transfer cannot be assumed to finish in one `send()` call.

## Parallel MIN Flow

The MIN program runs with:

```bash
./operations -t <num_threads> -f <file>
```

Simple explanation:

1. Read the binary file into an `int32_t` array.
2. Create `T` threads.
3. Split the array into `T` blocks.
4. Each thread scans one block and finds its local minimum.
5. A mutex protects the shared `global_min`.
6. A condition variable lets the main thread wait until all MIN threads finish.
7. Write the answer to:

```text
result_min.txt
```

The output format is:

```text
MIN=<value>
```

## Interview Explanation

For the server, I can say:

> My server is the producer. It waits for TCP connections on port 9090. For each client request, it forks a child process. The child receives which chunk is needed, seeks to the correct file offset, sends the required 8-byte header, then sends the chunk payload.

For the MIN analytics, I can say:

> I used data parallelism. The array is divided into contiguous blocks. Each thread computes a local minimum for its own block. Then a mutex protects the update to the shared global minimum. A condition variable is used to notify the main thread when the workers are done.
