# Demo Commands

Useful commands for demonstrating and verifying the OS Parallel File Transfer
and Analytics assignment.

## 1. Compile The Project

```bash
make
```

This creates the three executables:

```text
server
client
operations
```

`make` does not create `original.dat`, `reassembled.dat`, result files, or
`execution_log.txt`. Those files are created when you run the programs.

## 2. Create `original.dat` Bigger Than 1GB

Each `int32_t` value is 4 bytes.

```text
270,000,000 integers x 4 bytes = 1,080,000,000 bytes
```

This is larger than 1 GiB, which is `1,073,741,824` bytes.

```bash
python3 -c 'import random,struct; n=270000000; chunk=1000000; f=open("original.dat","wb");
for i in range(0,n,chunk):
    m=min(chunk,n-i)
    vals=[random.randint(-1000000000,1000000000) for _ in range(m)]
    f.write(struct.pack(f"{m}i", *vals))
f.close()'
```

Check the size:

```bash
ls -lh original.dat
wc -c original.dat
```

Expected exact byte size:

```text
1080000000 original.dat
```

## 3. Run Server, Client, And Operations

Terminal 1 - server/producer:

```bash
./server original.dat
```

Leave this terminal running. The server waits for client connections.

Terminal 2 - client/consumer:

```bash
./client -p 4
```

The client launches 4 child processes, reassembles the chunks into
`reassembled.dat`, then automatically runs:

```bash
./operations -t 8 -f reassembled.dat
```

Terminal 3 - optional manual operations demo:

```bash
./operations -t 8 -f reassembled.dat
```

This proves `operations` can run independently. It updates:

```text
result_min.txt
result_max.txt
result_sorted.dat
execution_log.txt
```

## 4. Verify File Transfer Correctness

Compare the original and reassembled files:

```bash
md5sum original.dat reassembled.dat
```

The two hashes must be identical.

Also check file size:

```bash
ls -lh original.dat reassembled.dat
```

Both files should show about `1.1G`.

## 5. Verify Analytics Output

```bash
cat result_min.txt
cat result_max.txt
cat execution_log.txt
```

Expected output format:

```text
MIN=<value>
MAX=<value>
[PART1] CHUNKS=4 | PROCS=4 | SYNC_USED=mutex,sem,condvar
[PART2] THREADS=8 | DATA_PARALLEL=min,max | TASK_PARALLEL=sort
[PART2] TIME_MS=<number> | SORT_ALGO=parallel_merge_sort
[STATUS] SUCCESS
```

To show hidden characters and prove the first characters are not missing:

```bash
cat -A result_min.txt result_max.txt execution_log.txt
```

The `$` shown by `cat -A` only means end of line.

## 6. Stop The Server

After client and verification are finished, go back to the server terminal and
press:

```text
Ctrl + C
```

This is normal because the server runs in a loop waiting for more connections.

## 7. Clean And Rebuild

Use this only when you want a fully fresh run:

```bash
make clean
make
```

Warning: `make clean` deletes `original.dat`. If your 1GB file already exists
and you want to keep it, do not run `make clean`.

To keep `original.dat` but remove old outputs:

```bash
rm -f reassembled.dat result_min.txt result_max.txt result_sorted.dat execution_log.txt
```

## 8. Optional Server Log

`server.log` is not required by the assignment. It is only useful if you want to
save server output.

Create it with:

```bash
./server original.dat > server.log 2>&1
```

Then run the client from another terminal.
