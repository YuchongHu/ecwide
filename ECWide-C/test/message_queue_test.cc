#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#define REQUEST 1
#define RESPONSE 2

typedef struct message {
  long int type;
  char data[128];
} message;

void rm_msg_queue(int msg_id, bool catch_error) {
  int s = msgctl(msg_id, IPC_RMID, nullptr);
  if (s < 0 && catch_error) {
    printf("msgctl error\n");
    exit(-1);
  }
}

int init_msg_queue() {
  const key_t key = 23333;
  rm_msg_queue(key, false);
  int s = msgget(key, IPC_CREAT | 0777);
  if (s < 0) {
    printf("msgget error\n");
    exit(-1);
  }
  return s;
}

void send_msg(int msg_id, bool is_req, char* data) {
  static message m;
  m.type = is_req ? REQUEST : RESPONSE;
  if (data) {
    memcpy(m.data, data, 128);
  } else {
    memset(m.data, 0, 128);
  }
  int s = msgsnd(msg_id, &m, 128, 0);
  if (s < 0) {
    printf("msgsnd error\n");
    exit(-1);
  }
}

void recv_msg(int msg_id, bool is_req, char* data) {
  static message m;
  if (!data) {
    printf("null data\n");
    exit(-1);
  }
  int s = msgrcv(msg_id, &m, 128, is_req ? REQUEST : RESPONSE, 0);
  if (s < 0) {
    printf("msgrcv error\n");
    exit(-1);
  } else {
    memcpy(data, m.data, 128);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("role is required: [s/c]\n");
    exit(-1);
  }
  int msg_id = init_msg_queue();
  printf("init_msg_queue ok\n");
  message m;
  if (argv[1][0] == 's') {
    while (true) {
      recv_msg(msg_id, true, m.data);
      printf("get request: %s\n", m.data);

      strcpy(m.data, "I got request.");
      send_msg(msg_id, false, m.data);
      printf("send response\ndeal ok\n\n");
    }
    rm_msg_queue(msg_id, true);
  } else {
    strcpy(m.data, "req__~~");
    send_msg(msg_id, true, m.data);
    printf("send request ok\n");
    recv_msg(msg_id, false, m.data);
    printf("get response: %s\n", m.data);
  }
  return 0;
}