#include "NativeCodec.h"

#include <jni.h>
#include <sys/time.h>

#include <iostream>
#include <thread>

#include "isa-l.h"
using namespace std;

JNIEXPORT void JNICALL Java_NativeCodec_generateEncodeMatrix__(JNIEnv* env,
                                                               jobject obj) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get encode matrix
  jfieldID fid_matrix =
      env->GetFieldID(class_codec, "encodeMatrix", "Ljava/nio/ByteBuffer;");
  jobject jba_matrix = (jobject)env->GetObjectField(obj, fid_matrix);
  uint8_t* matrix = (uint8_t*)env->GetDirectBufferAddress(jba_matrix);
  // get k & n
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_n = env->GetFieldID(class_codec, "n", "I");
  jint n = env->GetIntField(obj, fid_n);

  // generate the whole matrix
  gf_gen_cauchy1_matrix(matrix, n, k);
}

JNIEXPORT void JNICALL Java_NativeCodec_initEncodeTable(JNIEnv* env,
                                                        jobject obj) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get encode matrix
  jfieldID fid_matrix =
      env->GetFieldID(class_codec, "encodeMatrix", "Ljava/nio/ByteBuffer;");
  jobject jba_matrix = (jobject)env->GetObjectField(obj, fid_matrix);
  uint8_t* encode_matrix = (uint8_t*)env->GetDirectBufferAddress(jba_matrix);
  // get gf table
  jfieldID fid_gftbl =
      env->GetFieldID(class_codec, "gftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_gftbl = (jobject)env->GetObjectField(obj, fid_gftbl);
  uint8_t* gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_gftbl);
  // get k & m
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_m = env->GetFieldID(class_codec, "m", "I");
  jint m = env->GetIntField(obj, fid_m);

  // call isa-l init table
  ec_init_tables(k, m, encode_matrix, gftbl);

  // // notify the modification and write back
  // env->ReleaseByteArrayElements(jba_gftbl, (jbyte*)gftbl, 0);
}

JNIEXPORT void JNICALL Java_NativeCodec_initDecodeTable(JNIEnv* env,
                                                        jobject obj) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get decode gf table
  jfieldID fid_dgftbl =
      env->GetFieldID(class_codec, "decodeGftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_dgftbl = (jobject)env->GetObjectField(obj, fid_dgftbl);
  uint8_t* decode_gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_dgftbl);
  // get k
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);

  // if (decode_gftbl == nullptr) {
  //   printf("the decode_gftbl is nullptr!\n");
  // }

  // printf("get member ok\n");

  // all-1 line for xor
  uint8_t decode_matrix[k];
  int i;
  for (i = 0; i < k; ++i) {
    decode_matrix[i] = 1;
  }
  // call isa-l to get decode gf table
  ec_init_tables(k, 1, decode_matrix, decode_gftbl);

  // // notify the modification and write back
  // env->ReleaseByteArrayElements(jba_dgftbl, (jbyte*)decode_gftbl, 0);
  // delete[] decode_matrix;
}

JNIEXPORT void JNICALL Java_NativeCodec_encodeData(JNIEnv* env, jobject obj,
                                                   jobjectArray data_joa,
                                                   jobjectArray parity_joa) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get gf table
  jfieldID fid_gftbl =
      env->GetFieldID(class_codec, "gftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_gftbl = (jobject)env->GetObjectField(obj, fid_gftbl);
  uint8_t* gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_gftbl);
  // get k & m & chunk_size
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_m = env->GetFieldID(class_codec, "m", "I");
  jint m = env->GetIntField(obj, fid_m);
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  // get the data & parity memory space
  int i;
  jobject jba[k];
  uint8_t** data = new uint8_t*[k];
  for (i = 0; i < k; ++i) {
    jba[i] = env->GetObjectArrayElement(data_joa, i);
    data[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
    if (!data[i]) {
      printf("data[%d] is null!\n", i);
    }
  }
  uint8_t** parity = new uint8_t*[m];
  for (i = 0; i < m; ++i) {
    jba[i] = env->GetObjectArrayElement(parity_joa, i);
    parity[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
    if (!parity[i]) {
      printf("parity[%d] is null!\n", i);
    }
  }
  cout << "--- encode data :";
  struct timeval start_time, end_time;
  gettimeofday(&start_time, 0);

  // call isa-l to encode data
  ec_encode_data(chunk_size, k, m, gftbl, data, parity);

  gettimeofday(&end_time, 0);
  double time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                (end_time.tv_usec - start_time.tv_usec) / 1000.0;
  cout << time << " ---\n";

  // // notify the modification and write back
  // for (i = 0; i < m; ++i) {
  //   env->ReleaseByteArrayElements(jba[i], (jbyte*)parity[i], 0);
  // }
  delete[] data;
  delete[] parity;
}

JNIEXPORT void JNICALL Java_NativeCodec_decodeData(JNIEnv* env, jobject obj,
                                                   jobjectArray data_joa,
                                                   jobject target_jba) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get decode gf table
  jfieldID fid_dgftbl =
      env->GetFieldID(class_codec, "decodeGftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_dgftbl = (jobject)env->GetObjectField(obj, fid_dgftbl);
  uint8_t* decode_gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_dgftbl);
  // get k & chunk_size
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  // get the data & target memory space
  int i;
  jobject jba[k];
  uint8_t** data = new uint8_t*[k];
  for (i = 0; i < k; ++i) {
    jba[i] = env->GetObjectArrayElement(data_joa, i);
    data[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t** target = new uint8_t*[1];
  target[0] = (uint8_t*)env->GetDirectBufferAddress(target_jba);

  // call isa-l to decode
  ec_encode_data(chunk_size, k, 1, decode_gftbl, data, target);

  // // notify the modification and write back
  // env->ReleaseByteArrayElements(target_jba, (jbyte*)target[0], 0);
}

JNIEXPORT void JNICALL Java_NativeCodec_generateEncodeMatrix__I(JNIEnv* env,
                                                                jobject obj,
                                                                jint index) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get encode matrix
  jfieldID fid_matrix =
      env->GetFieldID(class_codec, "encodeMatrix", "Ljava/nio/ByteBuffer;");
  jobject jba_matrix = (jobject)env->GetObjectField(obj, fid_matrix);
  uint8_t* matrix = (uint8_t*)env->GetDirectBufferAddress(jba_matrix);
  // get k & n
  jfieldID fid_k = env->GetFieldID(class_codec, "k", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_n = env->GetFieldID(class_codec, "n", "I");
  jint n = env->GetIntField(obj, fid_n);
  jfieldID fid_egn = env->GetFieldID(class_codec, "encodeGroupNum", "I");
  jint encode_group_num = env->GetIntField(obj, fid_egn);

  // generate the whole matrix
  int real_k = encode_group_num * k, m = n - k, real_n = real_k + m,
      matrix_len = real_k * real_n;
  // uint8_t* matrix = new uint8_t[matrix_len];
  gf_gen_cauchy1_matrix(matrix, real_n, real_k);

  // set the first parity encode line as XOR
  int i, j;  //, start = real_k * (real_k - 1), end = real_k + start;
  int matrix_index;
  for (matrix_index = 0; matrix_index < k; ++matrix_index) {
    matrix[matrix_index] = 1;
  }
  // reorganize the matrix to store
  for (i = real_k * real_k + index * k; i < matrix_len - real_k; i += real_k) {
    for (j = 0; j < k; ++j) {
      matrix[matrix_index++] = matrix[i + j];
    }
  }

  // // write back (only the encode part)
  // env->SetByteArrayRegion(jba_matrix, 0, k * m, (jbyte*)matrix);
  // delete[] matrix;
}

// void xor_single(uint8_t* source, uint8_t* target, int chunk_size) {
//   // for (int j = 0; j < chunk_size; ++j) {
//   //   target[j] ^= source[j];
//   // }
//   ec_encode_data(chunk_size, 2, 1, gftbl, data, output);
// }

JNIEXPORT void JNICALL Java_NativeCodec_xorIntemediate(JNIEnv* env, jobject obj,
                                                       jobjectArray src_joa,
                                                       jobjectArray tar_joa) {
  static uint8_t gftbl[32 * 2];
  static bool flag = false;
  if (flag) {
    uint8_t a[] = {1, 1};
    ec_init_tables(2, 1, a, gftbl);
  }

  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get chunk_size & global_num
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  jfieldID fid_gn = env->GetFieldID(class_codec, "globalNum", "I");
  jint global_num = env->GetIntField(obj, fid_gn);
  // get the source & target memory space
  int i, j;
  jobject jba[global_num];
  uint8_t** source = new uint8_t*[global_num];
  for (i = 0; i < global_num; ++i) {
    jba[i] = env->GetObjectArrayElement(src_joa, i);
    source[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t** target = new uint8_t*[global_num];
  for (i = 0; i < global_num; ++i) {
    jba[i] = env->GetObjectArrayElement(tar_joa, i);
    target[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  cout << "--- xor intermediate :";
  struct timeval start_time, end_time;
  gettimeofday(&start_time, 0);

  // for (i = 0; i < global_num; ++i) {
  //   for (j = 0; j < chunk_size; ++j) {
  //     target[i][j] ^= source[i][j];
  //   }
  // }
  // thread threads[global_num];
  // for (i = 0; i < global_num; ++i) {
  //   threads[i] = thread(xor_single, source[i], target[i], chunk_size);
  // }
  // for (i = 0; i < global_num; ++i) {
  //   threads[i].join();
  // }

  uint8_t *data[2], *output[1];
  for (i = 0; i < global_num; ++i) {
    data[0] = source[i];
    output[0] = data[1] = target[i];
    ec_encode_data(chunk_size, 2, 1, gftbl, data, output);
  }

  // uint8_t *data[global_num][2], *output[global_num][1];
  // thread threads[global_num];
  // for (i = 0; i < global_num; ++i) {
  //   data[i][0] = source[i];
  //   output[i][0] = data[i][1] = target[i];
  //   threads[i] = thread([=, &data, &output] {
  //     ec_encode_data(chunk_size, 2, 1, gftbl, data[i], output[i]);
  //   });
  // }
  // for (i = 0; i < global_num; ++i) {
  //   threads[i].join();
  // }

  gettimeofday(&end_time, 0);
  double time = (end_time.tv_sec - start_time.tv_sec) * 1000.0 +
                (end_time.tv_usec - start_time.tv_usec) / 1000.0;
  cout << time << " ---\n";

  // // notify the modification and write back
  // for (i = 0; i < global_num; ++i) {
  //   env->ReleaseByteArrayElements(jba[i], (jbyte*)target[i], 0);
  // }
  delete[] source;
  delete[] target;
  flag = true;
}
