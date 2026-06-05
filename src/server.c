#include <arpa/inet.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define PORT 9090
#define BACKLOG 16
#define IO_BUFFER_SIZE 65536

static ssize_t send_all(int sockfd, const void *buffer, size_t length) {
    const char *data = buffer;
    size_t sent_total = 0;

    while (sent_total < length) {
        ssize_t sent = send(sockfd, data + sent_total, length - sent_total, 0);
        if (sent <= 0) {
            return -1;
        }
        sent_total += (size_t)sent;
    }

    return (ssize_t)sent_total;
}

static ssize_t recv_all(int sockfd, void *buffer, size_t length) {
    char *data = buffer;
    size_t received_total = 0;

    while (received_total < length) {
        ssize_t received = recv(sockfd, data + received_total,
                                length - received_total, 0);
        if (received <= 0) {
            return -1;
        }
        received_total += (size_t)received;
    }

    return (ssize_t)received_total;
}

static uint64_t host_to_network_u64(uint64_t value) {
    uint32_t high = htonl((uint32_t)(value >> 32));
    uint32_t low = htonl((uint32_t)(value & 0xffffffffu));
    return ((uint64_t)low << 32) | high;
}

static off_t get_file_size(const char *path) {
    struct stat st;

    if (stat(path, &st) != 0) {
        return -1;
    }

    return st.st_size;
}

static int send_chunk(int client_fd, const char *file_path,
                      uint32_t total_chunks, uint32_t chunk_number) {
    off_t file_size = get_file_size(file_path);
    off_t chunk_size;
    off_t offset;
    off_t remaining;
    uint32_t payload_size;
    uint32_t header[2];
    FILE *file;
    char buffer[IO_BUFFER_SIZE];
    uint32_t bytes_left;

    if (file_size <= 0 || total_chunks == 0 || chunk_number == 0 ||
        chunk_number > total_chunks) {
        fprintf(stderr, "Invalid chunk request\n");
        return -1;
    }

    chunk_size = (file_size + total_chunks - 1) / total_chunks;
    offset = (off_t)(chunk_number - 1) * chunk_size;
    remaining = file_size - offset;

    if (remaining < chunk_size) {
        payload_size = (uint32_t)remaining;
    } else {
        payload_size = (uint32_t)chunk_size;
    }

    file = fopen(file_path, "rb");
    if (file == NULL) {
        perror("fopen");
        return -1;
    }

    if (fseeko(file, offset, SEEK_SET) != 0) {
        perror("fseeko");
        fclose(file);
        return -1;
    }

    header[0] = htonl(chunk_number);
    header[1] = htonl(payload_size);

    if (send_all(client_fd, header, sizeof(header)) < 0) {
        perror("send header");
        fclose(file);
        return -1;
    }

    bytes_left = payload_size;
    while (bytes_left > 0) {
        size_t to_read = bytes_left < IO_BUFFER_SIZE ? bytes_left : IO_BUFFER_SIZE;
        size_t read_count = fread(buffer, 1, to_read, file);

        if (read_count == 0) {
            fprintf(stderr, "Unexpected end of file\n");
            fclose(file);
            return -1;
        }

        if (send_all(client_fd, buffer, read_count) < 0) {
            perror("send payload");
            fclose(file);
            return -1;
        }

        bytes_left -= (uint32_t)read_count;
    }

    fclose(file);
    return 0;
}

static void handle_client(int client_fd, const char *file_path) {
    uint32_t request[2];
    uint32_t total_chunks;
    uint32_t chunk_number;

    if (recv_all(client_fd, request, sizeof(request)) < 0) {
        perror("recv request");
        close(client_fd);
        exit(1);
    }

    total_chunks = ntohl(request[0]);
    chunk_number = ntohl(request[1]);

    if (total_chunks == 0 && chunk_number == 0) {
        off_t file_size = get_file_size(file_path);
        uint64_t file_size_net = host_to_network_u64((uint64_t)file_size);

        if (send_all(client_fd, &file_size_net, sizeof(file_size_net)) < 0) {
            perror("send file size");
            close(client_fd);
            exit(1);
        }

        close(client_fd);
        exit(0);
    }

    printf("Sending chunk %u of %u\n", chunk_number, total_chunks);

    if (send_chunk(client_fd, file_path, total_chunks, chunk_number) != 0) {
        close(client_fd);
        exit(1);
    }

    close(client_fd);
    exit(0);
}

int main(int argc, char *argv[]) {
    const char *file_path;
    int server_fd;
    struct sockaddr_in server_addr;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
        return 1;
    }

    file_path = argv[1];
    if (get_file_size(file_path) <= 0) {
        fprintf(stderr, "Cannot read input file: %s\n", file_path);
        return 1;
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, BACKLOG) < 0) {
        perror("listen");
        close(server_fd);
        return 1;
    }

    printf("Server listening on port %d\n", PORT);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        pid_t child_pid;

        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        child_pid = fork();
        if (child_pid < 0) {
            perror("fork");
            close(client_fd);
            continue;
        }

        if (child_pid == 0) {
            close(server_fd);
            handle_client(client_fd, file_path);
        }

        close(client_fd);

        while (waitpid(-1, NULL, WNOHANG) > 0) {
        }
    }

    close(server_fd);
    return 0;
}
