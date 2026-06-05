# My Responsibility Notes

## My Part

For my part, I worked mainly on the server side of the file transfer.

My responsibilities:

1. Build the server/producer program.
2. Split the input file into chunks.
3. Send the requested chunk through TCP.
4. Use `fork()` so each connection is handled by a child process.
5. Help with file integrity checking using `md5sum`.

The folder also has the client and operations files so I can test the whole
assignment flow before merging with my group.

## Server Flow

The server runs like this:

```bash
./server original.dat
```

What the server does:

1. Checks that the input file exists.
2. Creates a TCP socket.
3. Binds the socket to port `9090`.
4. Listens for client connections.
5. Accepts a connection.
6. Forks a child process.
7. The child receives which chunk the client wants.
8. The child seeks to the correct file offset.
9. The child sends the chunk header and payload.
10. The child exits after sending.

## Request Format

The client sends two numbers to the server:

```text
total chunks
chunk number
```

Both are sent in network byte order.

## Response Format

The server sends the chunk using the format required in the assignment:

```text
4 bytes: sequence number
4 bytes: payload size
payload: raw int32_t data
```

The sequence number is used by the client to put the chunk back in the correct
position.

## Chunk Calculation

The chunk size is calculated like this:

```c
chunk_size = (file_size + total_chunks - 1) / total_chunks;
```

I used this because it rounds up and also handles the last chunk properly when
the file size is not evenly divisible by the number of chunks.

The offset is:

```c
offset = (chunk_number - 1) * chunk_size;
```

That tells the child process where to start reading from the file.

## Why I Used `send_all()`

I used `send_all()` because `send()` does not always send the full buffer in one
call. For a large file, this matters because each chunk can be very big.

`send_all()` keeps looping until all bytes are sent.

## How I Would Explain This

For the interview, I would explain it like this:

> My server acts as the producer. It listens on port 9090 and waits for client
> requests. When a client connects, the server forks a child process. The child
> receives the requested chunk number, calculates the file offset, reads that
> part of the file, then sends the required 8-byte header and payload through
> the socket.

For validation, I would say:

> After the client reassembles the file, I used `md5sum original.dat
> reassembled.dat` to confirm both files are exactly the same.
