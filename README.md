# Parallel File Transfer & Multithreaded Analytics

A Linux-based **C networking and operating systems project** that transfers a binary file over TCP using multiple processes, reconstructs it in shared memory, and performs parallel data analytics on the received file.

The project demonstrates **TCP sockets, `fork()`, shared memory (`mmap`), POSIX synchronization, and multithreading (`pthreads`)** in one end-to-end workflow.

## Project highlights

- **Parallel file transfer:** Divide a file into chunks and request them concurrently with multiple client processes.
- **TCP client/server:** Transfer chunks using a custom binary protocol on port `9090`.
- **Shared-memory reassembly:** Place incoming chunks at their correct offsets before writing `reassembled.dat`.
- **Process synchronization:** Coordinate workers using a process-shared mutex, semaphore, and condition variable.
- **Parallel analytics:** Calculate minimum and maximum values and sort binary `int32_t` data with multiple threads.
- **Verification:** Compare the original and reconstructed files and inspect the result files and execution log.

## Architecture

```text
                   TCP port 9090
  original.dat  ───►  C SERVER (server)
                          │
                  serves requested chunks
                          │
                     C CLIENT (client)
                ┌─────────┼─────────┐
                ▼         ▼         ▼
             Process 1  Process 2   ... Process N
                └─────────┼─────────┘
                          ▼
                Shared mmap buffer
                          ▼
                   reassembled.dat
                          ▼
              C ANALYTICS (operations)
                          ▼
             MIN / MAX / Parallel Merge Sort
```

The client launches the analytics executable automatically after a successful transfer, using **8 threads** for the analytics stage.

## Tech stack

| Area | Technologies |
| --- | --- |
| Language | C (C99) |
| Operating system | Linux (tested on Debian) |
| Networking | TCP/IP, POSIX sockets |
| Parallel processes | `fork()`, `waitpid()` |
| Shared memory | `mmap()` |
| Synchronization | POSIX mutexes, semaphores, condition variables |
| Multithreading | `pthread_create()`, `pthread_join()` |
| Tooling | GCC, Make, Git, Python 3 for sample data generation |

## Requirements

A Linux environment with GCC, Make, POSIX threading support, Python 3, and Git. Debian and Ubuntu are suitable.

On Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y build-essential python3 git
```

## Installation

```bash
git clone https://github.com/JasonTan21/Parallel-File-Transfer.git
cd Parallel-File-Transfer
make
```

This builds three executables in the repository root:

```text
server
client
operations
```

## Quick start: reproduce the successful demo

### 1. Generate a small binary test file

From the repository root:

```bash
python3 -c 'import struct; open("original.dat", "wb").write(struct.pack("<1000i", *range(1000)))'
```

This produces 1,000 signed 32-bit integers (0 through 999) in a 4,000-byte binary file.

### 2. Start the server (Terminal 1)

```bash
./server original.dat
```

Leave this running while the client connects.

### 3. Start the client (Terminal 2)

Open a second terminal in the **same project directory**:

```bash
./client -p 4 -h 127.0.0.1
```

This requests **4 chunks using 4 child processes**, reassembles them, and automatically executes the analytics stage with **8 threads**.

### 4. Verify the transfer and results

```bash
cmp original.dat reassembled.dat && echo "Transfer successful"
cat result_min.txt
cat result_max.txt
cat execution_log.txt
```

**Observed result from the Debian test:**

```text
Transfer successful
MIN=0
MAX=999
[PART1] CHUNKS=4 | PROCS=4 | SYNC_USED=mutex,sem,condvar
[PART2] THREADS=8 | DATA_PARALLEL=min,max | TASK_PARALLEL=sort
[PART2] TIME_MS=1 | SORT_ALGO=parallel_merge_sort
[STATUS] SUCCESS
```

Execution time varies by system and file size. The example above is from a small local test, **not a large-file performance benchmark**.

Stop the server with `Ctrl+C` when finished.

## Program usage

| Program | Command | Purpose |
| --- | --- | --- |
| Server | `./server original.dat` | Serve an existing binary file on TCP port 9090 |
| Client | `./client -p 4 -h 127.0.0.1` | Download with 4 parallel processes |
| Analytics | `./operations -t 8 -f reassembled.dat` | Run analytics independently with 8 threads |

The client accepts a **numeric IPv4 address** after `-h`. In the current version, `-h localhost` is not supported; use `127.0.0.1` for local testing.

## Transfer protocol

Before downloading chunks, the client sends a metadata request with `total_chunks = 0` and `chunk_number = 0`. The server responds with the file size, allowing the client to allocate a shared reassembly buffer.

For each requested chunk, the response contains:

```text
4 bytes: chunk sequence number
4 bytes: payload size
N bytes: chunk payload
```

The client writes each payload into the appropriate position in the shared buffer and then writes the complete file to disk.

## Generated files

| File | Description |
| --- | --- |
| `original.dat` | Input binary file created for testing |
| `reassembled.dat` | Reconstructed output from the client |
| `result_min.txt` | Minimum integer value |
| `result_max.txt` | Maximum integer value |
| `result_sorted.dat` | Sorted binary integer data |
| `execution_log.txt` | Process count, synchronization, thread count, timing, and status |

These generated files are not intended to be committed to Git.

## Project structure

```text
Parallel-File-Transfer/
├── src/
│   ├── server.c
│   ├── client.c
│   └── operations.c
├── Makefile
├── DEMO_COMMANDS.md
├── README.md
└── .gitignore
```

For additional demonstrations, including generation of a file larger than 1 GB, see [`DEMO_COMMANDS.md`](DEMO_COMMANDS.md). The large-file workflow has different memory and runtime requirements and is **not demonstrated by the small test above**.

**Note:** The current `Makefile` references `run_test.sh` through `make test`, but that script is not included in this repository. Use the manual verification steps above until the test script is added.

## Security and limitations

- The TCP server currently has **no authentication, transport encryption, or access controls**. Run it on `127.0.0.1` or a trusted/private network only; do not expose port 9090 directly to the public internet.
- The data-analytics stage expects a raw binary file of 32-bit signed integers, not an arbitrary document format.
- The example demonstrates the project's local client/server workflow; transferring data between separate machines requires a reachable private IPv4 address and appropriate network configuration.
- Results from a small demonstration do not establish throughput or stability for 1 GB+ transfers.

## Portfolio

This is an **Operating Systems / Systems Programming** project demonstrating concurrency, network communication, inter-process synchronization, and multithreaded processing.

**Developer:** [JasonTan21](https://github.com/JasonTan21), [kaydenchong7-blip](https://github.com/kaydenchong7-blip), [Nadota](https://github.com/Nadota)
**Portfolio:** [folio.theusefulhacker.my](https://folio.theusefulhacker.my/)  
**Source:** [Parallel-File-Transfer](https://github.com/JasonTan21/Parallel-File-Transfer)
