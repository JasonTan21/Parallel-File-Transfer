#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define SERVER_IP "127.0.0.1"
#define PORT 9090
#define IO_BUFFER 65536

struct shared_data
{
    pthread_mutex_t lock;
    pthread_cond_t cond;
    sem_t sem;
    int chunks_done;
};

uint64_t fetch_file_size()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("connect error");
        exit(1);
    }

    uint32_t req[2] = {0, 0};
    send(sock, req, sizeof(req), 0);

    uint64_t net_size;
    recv(sock, &net_size, sizeof(net_size), 0);
    close(sock);

    uint32_t high = ntohl((uint32_t)(net_size >> 32));
    uint32_t low = ntohl((uint32_t)(net_size & 0xFFFFFFFF));
    return ((uint64_t)high << 32) | low;
}

int main(int argc, char *argv[])
{
    int num_chunks = 0;

    if (argc == 3 && strcmp(argv[1], "-p") == 0)
    {
        num_chunks = atoi(argv[2]);
    }
    else
    {
        printf("Usage: ./client -p <N>\n");
        return 1;
    }

    uint64_t file_size = fetch_file_size();
    printf("Target file size: %lu bytes\n", file_size);
    uint64_t chunk_size = (file_size + num_chunks - 1) / num_chunks;

    struct shared_data *sync_vars = mmap(NULL, sizeof(struct shared_data),
                                         PROT_READ | PROT_WRITE,
                                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&sync_vars->lock, &mattr);

    pthread_condattr_t cattr;
    pthread_condattr_init(&cattr);
    pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(&sync_vars->cond, &cattr);

    sem_init(&sync_vars->sem, 1, 0);
    sync_vars->chunks_done = 0;

    char *file_buffer = mmap(NULL, file_size, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    for (int i = 1; i <= num_chunks; i++)
    {
        if (fork() == 0)
        {

            int sock = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in serv_addr;
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(PORT);
            inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

            connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

            uint32_t req[2] = {htonl(num_chunks), htonl(i)};
            send(sock, req, sizeof(req), 0);

            uint32_t header[2];
            recv(sock, header, sizeof(header), MSG_WAITALL);
            uint32_t seq_num = ntohl(header[0]);
            uint32_t payload_size = ntohl(header[1]);

            uint64_t offset = (seq_num - 1) * chunk_size;

            uint32_t bytes_left = payload_size;
            char *write_ptr = file_buffer + offset;

            while (bytes_left > 0)
            {
                int to_read = bytes_left < IO_BUFFER ? bytes_left : IO_BUFFER;
                int received = recv(sock, write_ptr, to_read, 0);
                if (received <= 0)
                {
                    perror("recv failed in child");
                    close(sock);
                    exit(1);
                }
                write_ptr += received;
                bytes_left -= received;
            }
            close(sock);

            pthread_mutex_lock(&sync_vars->lock);
            sync_vars->chunks_done++;
            if (sync_vars->chunks_done == num_chunks)
            {
                pthread_cond_signal(&sync_vars->cond);
            }
            pthread_mutex_unlock(&sync_vars->lock);

            sem_post(&sync_vars->sem);

            exit(0);
        }
    }

    pthread_mutex_lock(&sync_vars->lock);
    while (sync_vars->chunks_done < num_chunks)
    {
        pthread_cond_wait(&sync_vars->cond, &sync_vars->lock);
    }
    pthread_mutex_unlock(&sync_vars->lock);

    for (int i = 0; i < num_chunks; i++)
    {
        wait(NULL);
    }

    FILE *out_file = fopen("reassembled.dat", "wb");
    fwrite(file_buffer, 1, file_size, out_file);
    fclose(out_file);
    printf("Reassembly complete. Wrote to reassembled.dat\n");

    munmap(file_buffer, file_size);
    sem_destroy(&sync_vars->sem);
    pthread_mutex_destroy(&sync_vars->lock);
    pthread_cond_destroy(&sync_vars->cond);
    munmap(sync_vars, sizeof(struct shared_data));

    if (fork() == 0)
    {
        execl("./operations", "operations", "-t", "4", "-f", "reassembled.dat", NULL);
        perror("execl failed");
        exit(1);
    }
    wait(NULL);

    return 0;
}
