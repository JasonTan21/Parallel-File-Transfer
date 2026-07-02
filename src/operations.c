/*
 * operations.c — CNS6214 Part 2: Multi-Threaded File Analytics
 *
 * Usage: ./operations -t <num_threads> -f <file>
 *
 * Reads a binary file of int32_t values and computes:
 *   - global minimum   (DATA PARALLELISM)
 *   - global maximum    (DATA PARALLELISM)
 *   - ascending sort    (DATA PARALLELISM for leaf sorts + TASK PARALLELISM for merges)
 *
 * Outputs:
 *   result_min.txt     "MIN=<value>\n"
 *   result_max.txt     "MAX=<value>\n"
 *   result_sorted.dat  raw int32_t binary, ascending
 *   execution_log.txt  appended PART2 lines + STATUS line
 *
 * Synchronization used:
 *   pthread_mutex_t  - protects global min/max accumulators and the
 *                       per-level "threads remaining" counter during merge sort
 *   sem_t            - caps the number of merge tasks allowed to run their
 *                       critical merge work concurrently (workload throttling)
 *   pthread_cond_t   - lets the main thread block until all merge tasks at the
 *                       current level have finished, before starting the next level
 */

#define _GNU_SOURCE          /* unlocks a few extra non-standard declarations */
#include <stdio.h>           /* fopen/fprintf/fwrite — file + text output */
#include <stdlib.h>          /* malloc, qsort, atoi */
#include <string.h>          /* memcpy, strerror */
#include <stdint.h>          /* int32_t — guarantees an exact 32-bit integer */
#include <unistd.h>          /* low-level open()/close(), getopt() */
#include <fcntl.h>           /* O_RDONLY flag for open() */
#include <errno.h>           /* errno — "why did that last call fail?" */
#include <time.h>            /* clock_gettime — for timing the run */
#include <pthread.h>         /* threads, mutexes, condition variables */
#include <semaphore.h>       /* sem_t — the semaphore tool */
#include <sys/mman.h>        /* mmap — map the file straight into memory */
#include <sys/stat.h>        /* fstat — ask the OS how big the file is */
#include <sys/types.h>

/* ----------------------------------------------------------------------- */
/* Globals                                                                  */
/*                                                                           */
/* Everything below is declared OUTSIDE any function, which in C means     */
/* every thread we create can see and touch it. That's exactly what we     */
/* need: threads share these variables instead of each getting a private   */
/* copy, which is how they coordinate and report results back.             */
/* ----------------------------------------------------------------------- */

static int32_t *g_data   = NULL; /* mmap'd, READ-ONLY view of the source file. We
                                   * never modify this — it's the ground truth. */
static int32_t *g_sorted = NULL; /* a separate, writable copy we are allowed to
                                   * rearrange while sorting. */
static int32_t *g_temp   = NULL; /* scratch space borrowed temporarily while
                                   * merging two sorted halves together. */
static size_t   g_n      = 0;    /* how many int32_t numbers are in the file */
static int      g_T      = 1;    /* how many threads the user asked for (-t) */

/* The two answers we're building toward. They start at the most extreme
 * possible values so that the very first real number we look at will
 * immediately beat them and take their place. */
static int32_t global_min = INT32_MAX;
static int32_t global_max = INT32_MIN;

/* Lock that protects global_min/global_max from being corrupted if two
 * threads try to update them at the exact same instant (a race condition). */
static pthread_mutex_t minmax_lock = PTHREAD_MUTEX_INITIALIZER;

/* ---- The matching set of synchronization tools for the SORT stage ---- */
static sem_t merge_sem;                 /* caps how many merges run their heavy
                                          * work at the same time (a "bouncer"
                                          * that only lets g_T in at once)   */
static pthread_mutex_t level_lock = PTHREAD_MUTEX_INITIALIZER; /* protects the
                                          * counter below from being decremented
                                          * by two threads simultaneously     */
static pthread_cond_t  level_cond = PTHREAD_COND_INITIALIZER;  /* lets the main
                                          * thread sleep until it's woken up,
                                          * instead of burning CPU checking in
                                          * a loop                            */
static int level_remaining = 0;         /* how many merge tasks are still
                                          * unfinished in the current round   */

/* ----------------------------------------------------------------------- */
/* Utility                                                                  */
/* ----------------------------------------------------------------------- */

static void die(const char *msg) {
    fprintf(stderr, "operations: %s: %s\n", msg, strerror(errno));
    exit(1);
}

static void usage(const char *prog) {
    fprintf(stderr, "Usage: %s -t <num_threads> -f <file>\n", prog);
    exit(1);
}

static long elapsed_ms(struct timespec *start, struct timespec *end) {
    long sec  = end->tv_sec  - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;
    return sec * 1000L + nsec / 1000000L;
}

static int cmp_i32(const void *a, const void *b) {
    int32_t x = *(const int32_t *)a;
    int32_t y = *(const int32_t *)b;
    return (x > y) - (x < y);
}

/* =========================================================================
 * DATA PARALLELISM: Min / Max
 * Each of g_T threads scans one contiguous block of g_data and computes a
 * local min and local max. The only shared, mutable state is the pair of
 * global accumulators, which is protected by minmax_lock during the brief
 * reduction step — this is the race condition we are guarding against.
 * ========================================================================= */

typedef struct {
    size_t start, end; /* [start, end) range in g_data */
} range_t;

static void *minmax_worker(void *arg) {
    range_t *r = (range_t *)arg;
    int32_t local_min = INT32_MAX;
    int32_t local_max = INT32_MIN;

    for (size_t i = r->start; i < r->end; i++) {
        int32_t v = g_data[i];
        if (v < local_min) local_min = v;
        if (v > local_max) local_max = v;
    }

    /* ---- critical section: reduce into global accumulators ---- */
    pthread_mutex_lock(&minmax_lock);
    if (local_min < global_min) global_min = local_min;
    if (local_max > global_max) global_max = local_max;
    pthread_mutex_unlock(&minmax_lock);
    /* ------------------------------------------------------------ */

    return NULL;
}

static void compute_min_max(void) {
    pthread_t *tids = malloc(sizeof(pthread_t) * g_T);
    range_t   *ranges = malloc(sizeof(range_t) * g_T);
    if (!tids || !ranges) die("malloc (minmax)");

    size_t block = (g_n + g_T - 1) / g_T; /* contiguous block size per thread */
    int created = 0;

    for (int i = 0; i < g_T; i++) {
        size_t start = (size_t)i * block;
        if (start >= g_n) break;
        size_t end = start + block;
        if (end > g_n) end = g_n;
        ranges[i].start = start;
        ranges[i].end   = end;
        if (pthread_create(&tids[i], NULL, minmax_worker, &ranges[i]) != 0)
            die("pthread_create (minmax)");
        created++;
    }
    for (int i = 0; i < created; i++)
        pthread_join(tids[i], NULL);

    free(tids);
    free(ranges);
}

/* =========================================================================
 * Parallel Merge Sort
 *
 * Phase A — DATA PARALLELISM (leaf sort):
 *   g_sorted is split into g_T contiguous chunks. One thread per chunk
 *   sorts its own chunk independently with no shared mutable state at all,
 *   so no locking is needed in this phase.
 *
 * Phase B — TASK PARALLELISM (bottom-up merge):
 *   At each merge level, every adjacent pair of sorted runs is an
 *   independent merge TASK. Multiple merge tasks for the same level run
 *   concurrently as separate threads. A semaphore (merge_sem) throttles how
 *   many of those tasks may be doing the actual memory-bound merge work at
 *   once (workload control). A mutex + condition variable implement a
 *   "level barrier": the main thread blocks on pthread_cond_wait until the
 *   last task of the level decrements level_remaining to 0 and signals,
 *   guaranteeing level L+1 never starts merging runs that level L hasn't
 *   finished producing.
 * ========================================================================= */

typedef struct {
    size_t left_start, mid, right_end;
} merge_task_t;

static void merge_range(size_t left_start, size_t mid, size_t right_end) {
    size_t i = left_start, j = mid, k = left_start;
    while (i < mid && j < right_end) {
        if (g_sorted[i] <= g_sorted[j]) g_temp[k++] = g_sorted[i++];
        else                            g_temp[k++] = g_sorted[j++];
    }
    while (i < mid)       g_temp[k++] = g_sorted[i++];
    while (j < right_end) g_temp[k++] = g_sorted[j++];
    memcpy(&g_sorted[left_start], &g_temp[left_start],
           (right_end - left_start) * sizeof(int32_t));
}

static void *merge_worker(void *arg) {
    merge_task_t *t = (merge_task_t *)arg;

    sem_wait(&merge_sem);                 /* throttle concurrent merge work */
    merge_range(t->left_start, t->mid, t->right_end);
    sem_post(&merge_sem);

    /* ---- level barrier: signal that this task is done ---- */
    pthread_mutex_lock(&level_lock);
    level_remaining--;
    if (level_remaining == 0)
        pthread_cond_signal(&level_cond);
    pthread_mutex_unlock(&level_lock);
    /* ------------------------------------------------------- */

    free(t);
    return NULL;
}

typedef struct {
    size_t start, end;
} leaf_t;

static void *leaf_sort_worker(void *arg) {
    leaf_t *l = (leaf_t *)arg;
    qsort(&g_sorted[l->start], l->end - l->start, sizeof(int32_t), cmp_i32);
    return NULL;
}

static void parallel_merge_sort(void) {
    /* Copy source data into the writable working buffer. */
    memcpy(g_sorted, g_data, g_n * sizeof(int32_t));

    size_t chunk = (g_n + g_T - 1) / g_T;
    if (chunk == 0) chunk = g_n > 0 ? g_n : 1;

    /* ---------------- Phase A: DATA PARALLELISM (leaf sort) ---------------- */
    int      nleaf = 0;
    pthread_t *leaf_tids = malloc(sizeof(pthread_t) * g_T);
    leaf_t    *leaves     = malloc(sizeof(leaf_t) * g_T);
    if (!leaf_tids || !leaves) die("malloc (leaves)");

    for (int i = 0; i < g_T; i++) {
        size_t start = (size_t)i * chunk;
        if (start >= g_n) break;
        size_t end = start + chunk;
        if (end > g_n) end = g_n;
        leaves[nleaf].start = start;
        leaves[nleaf].end   = end;
        if (pthread_create(&leaf_tids[nleaf], NULL, leaf_sort_worker, &leaves[nleaf]) != 0)
            die("pthread_create (leaf sort)");
        nleaf++;
    }
    for (int i = 0; i < nleaf; i++)
        pthread_join(leaf_tids[i], NULL);

    free(leaf_tids);

    /* ---------------- Phase B: TASK PARALLELISM (bottom-up merge) ---------------- */
    /* Each run starts at size `chunk` (the leaf size); runs double each level. */
    for (size_t run = chunk; run < g_n; run *= 2) {
        /* Build the list of independent merge tasks for this level. */
        size_t max_pairs = (g_n / run) + 1;
        pthread_t *tids = malloc(sizeof(pthread_t) * max_pairs);
        if (!tids) die("malloc (merge tids)");

        int ntasks = 0;
        for (size_t left_start = 0; left_start < g_n; left_start += 2 * run) {
            size_t mid = left_start + run;
            if (mid >= g_n) break; /* nothing to merge with on the right */
            ntasks++;
        }

        pthread_mutex_lock(&level_lock);
        level_remaining = ntasks;
        pthread_mutex_unlock(&level_lock);

        int ti = 0;
        for (size_t left_start = 0; left_start < g_n; left_start += 2 * run) {
            size_t mid = left_start + run;
            if (mid >= g_n) break;
            size_t right_end = left_start + 2 * run;
            if (right_end > g_n) right_end = g_n;

            merge_task_t *t = malloc(sizeof(merge_task_t));
            if (!t) die("malloc (merge task)");
            t->left_start = left_start;
            t->mid        = mid;
            t->right_end  = right_end;

            if (pthread_create(&tids[ti], NULL, merge_worker, t) != 0)
                die("pthread_create (merge)");
            ti++;
        }

        /* Block until every merge task for this level has signalled completion. */
        pthread_mutex_lock(&level_lock);
        while (level_remaining > 0)
            pthread_cond_wait(&level_cond, &level_lock);
        pthread_mutex_unlock(&level_lock);

        for (int i = 0; i < ti; i++)
            pthread_join(tids[i], NULL);

        free(tids);
    }
}

/* ----------------------------------------------------------------------- */
/* Output                                                                   */
/* ----------------------------------------------------------------------- */

static void write_min_max(void) {
    FILE *fmin = fopen("result_min.txt", "w");
    if (!fmin) die("fopen result_min.txt");
    fprintf(fmin, "MIN=%d\n", global_min);
    fclose(fmin);

    FILE *fmax = fopen("result_max.txt", "w");
    if (!fmax) die("fopen result_max.txt");
    fprintf(fmax, "MAX=%d\n", global_max);
    fclose(fmax);
}

static void write_sorted(void) {
    FILE *f = fopen("result_sorted.dat", "wb");
    if (!f) die("fopen result_sorted.dat");
    size_t written = fwrite(g_sorted, sizeof(int32_t), g_n, f);
    if (written != g_n) die("fwrite result_sorted.dat (short write)");
    fclose(f);
}

static void append_execution_log(int T, long time_ms) {
    /* Appended so it combines with the [PART1] line already written by the
     * client process, producing the exact multi-line template required by
     * the grader. */
    FILE *f = fopen("execution_log.txt", "a");
    if (!f) die("fopen execution_log.txt");
    fprintf(f, "[PART2] THREADS=%d | DATA_PARALLEL=min,max | TASK_PARALLEL=sort\n", T);
    fprintf(f, "[PART2] TIME_MS=%ld | SORT_ALGO=parallel_merge_sort\n", time_ms);
    fprintf(f, "[STATUS] SUCCESS\n");
    fclose(f);
}

/* ----------------------------------------------------------------------- */
/* main                                                                      */
/* ----------------------------------------------------------------------- */

int main(int argc, char **argv) {
    int T = 0;
    const char *filename = NULL;
    int opt;

    while ((opt = getopt(argc, argv, "t:f:")) != -1) {
        switch (opt) {
            case 't': T = atoi(optarg); break;
            case 'f': filename = optarg; break;
            default: usage(argv[0]);
        }
    }
    if (T <= 0 || filename == NULL) usage(argv[0]);
    g_T = T;

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* ---- Open & mmap the input file ---- */
    int fd = open(filename, O_RDONLY);
    if (fd < 0) die("open input file");

    struct stat st;
    if (fstat(fd, &st) != 0) die("fstat");
    if (st.st_size <= 0 || st.st_size % (off_t)sizeof(int32_t) != 0) {
        fprintf(stderr, "operations: file size %lld is not a multiple of %zu bytes\n",
                (long long)st.st_size, sizeof(int32_t));
        return 1;
    }
    g_n = (size_t)st.st_size / sizeof(int32_t);

    void *map = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map == MAP_FAILED) die("mmap");
    g_data = (int32_t *)map;
    close(fd);

    g_sorted = malloc(g_n * sizeof(int32_t));
    g_temp   = malloc(g_n * sizeof(int32_t));
    if (!g_sorted || !g_temp) die("malloc (sort buffers)");

    /* Semaphore caps concurrent merge work to g_T "slots". */
    if (sem_init(&merge_sem, 0, (unsigned)g_T) != 0) die("sem_init");

    /* ---- Run analytics ---- */
    compute_min_max();
    parallel_merge_sort();

    /* ---- Write outputs ---- */
    write_min_max();
    write_sorted();

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    append_execution_log(g_T, elapsed_ms(&t_start, &t_end));

    fprintf(stderr, "operations: OK — n=%zu threads=%d min=%d max=%d time_ms=%ld\n",
            g_n, g_T, global_min, global_max, elapsed_ms(&t_start, &t_end));

    munmap(map, (size_t)st.st_size);
    free(g_sorted);
    free(g_temp);
    sem_destroy(&merge_sem);

    return 0;
}
