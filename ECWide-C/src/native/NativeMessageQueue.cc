#include "NativeMessageQueue.h"

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

JNIEXPORT jint JNICALL Java_NativeMessageQueue_initMsgQueue(JNIEnv* env,
                                                            jobject obj) {
  const key_t key = 23333;
  rm_msg_queue(key, false);
  int s = msgget(key, IPC_CREAT | 0777);
  if (s < 0) {
    printf("msgget error\n");
    exit(-1);
  }
  return s;
}

JNIEXPORT void JNICALL Java_NativeMessageQueue_removeMsgQueue(
    JNIEnv* env, jobject obj, jint msg_queue_id) {
  rm_msg_queue(msg_queue_id, true);
}

JNIEXPORT void JNICALL Java_NativeMessageQueue_sendMsg(JNIEnv* env, jobject obj,
                                                       jint msg_queue_id,
                                                       jboolean is_req) {
  jclass class_msg_q = env->GetObjectClass(obj);
  jfieldID fid_bf =
      env->GetFieldID(class_msg_q, "buffer", "Ljava/nio/ByteBuffer;");
  jobject jba_bf = (jobject)env->GetObjectField(obj, fid_bf);
  char* data = (char*)env->GetDirectBufferAddress(jba_bf);

  static message m;
  m.type = is_req ? REQUEST : RESPONSE;
  memcpy(m.data, data, 128);
  int s = msgsnd(msg_queue_id, &m, 128, 0);
  if (s < 0) {
    printf("msgsnd error\n");
    exit(-1);
  }
}

JNIEXPORT void JNICALL Java_NativeMessageQueue_recvMsg(JNIEnv* env, jobject obj,
                                                       jint msg_queue_id,
                                                       jboolean is_req) {
  jclass class_msg_q = env->GetObjectClass(obj);
  jfieldID fid_bf =
      env->GetFieldID(class_msg_q, "buffer", "Ljava/nio/ByteBuffer;");
  jobject jba_bf = (jobject)env->GetObjectField(obj, fid_bf);
  char* data = (char*)env->GetDirectBufferAddress(jba_bf);

  static message m;
  int s = msgrcv(msg_queue_id, &m, 128, is_req ? REQUEST : RESPONSE, 0);
  if (s < 0) {
    printf("msgrcv error\n");
    exit(-1);
  } else {
    memcpy(data, m.data, 128);
  }
}