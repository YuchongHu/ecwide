#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#define PIPE_REQ "/tmp/pipe_req"
#define PIPE_RESPONSE "/tmp/pipe_ans"

void create_pipe(const char* pipe_name) {
  if (access(pipe_name, F_OK) == -1) {
    int res = mkfifo(pipe_name, 0777);
    if (res != 0) {
      fprintf(stderr, "Could not create fifo %s\n", pipe_name);
      exit(EXIT_FAILURE);
    }
  }
}

int open_pipe(const char* pipe_name, bool is_read) {
  return open(pipe_name, is_read ? O_RDONLY : O_WRONLY);
}

int read_pipe(int pipe_fd) {
  static char buffer[4];
  return read(pipe_fd, buffer, 1);
}

int write_pipe(int pipe_fd) {
  static char buffer[4] = {1};
  return write(pipe_fd, buffer, 1);
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "need role [r/w]\n");
    exit(-1);
  }
  if (argv[1][0] == 'r') {
    create_pipe(PIPE_REQ);
    create_pipe(PIPE_RESPONSE);
    int req_pipe = open_pipe(PIPE_REQ, true);
    int status;
    for (int i = 0; i < 5; ++i) {
      status = read_pipe(req_pipe);
      printf("read from pipe\n");
    }
    close(req_pipe);

  } else {
    int req_pipe = open_pipe(PIPE_REQ, false);
    int status;
    for (int i = 0; i < 5; ++i) {
      status = write_pipe(req_pipe);
      printf("write to pipe\n");
      sleep(1);
    }
    close(req_pipe);
  }
  return 0;
}