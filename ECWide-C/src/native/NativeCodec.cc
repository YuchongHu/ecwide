#include "NativeCodec.h"

#include <jni.h>
#include <sys/time.h>

#include <cstring>
#include <iostream>

#include "isa-l.h"
using namespace std;

JNIEXPORT void JNICALL Java_NativeCodec_generateEncodeMatrix(JNIEnv* env,
                                                             jobject obj) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get encode matrix
  jfieldID fid_matrix =
      env->GetFieldID(class_codec, "encodeMatrix", "Ljava/nio/ByteBuffer;");
  jobject jba_matrix = (jobject)env->GetObjectField(obj, fid_matrix);
  uint8_t* matrix = (uint8_t*)env->GetDirectBufferAddress(jba_matrix);
  // get k & n
  jfieldID fid_k = env->GetFieldID(class_codec, "encodeDataNum", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_m = env->GetFieldID(class_codec, "globalNum", "I");
  jint m = env->GetIntField(obj, fid_m);
  jint n = m + k;
  jfieldID fid_ct = env->GetFieldID(class_codec, "codeType", "C");
  jchar code_type = env->GetIntField(obj, fid_ct);

  // generate the whole matrix
  uint8_t* tmp_matrix = new uint8_t[k * n]();
  gf_gen_cauchy1_matrix(tmp_matrix, n, k);
  if (code_type != 'C') {
    memcpy(matrix, tmp_matrix + k * k, k * m);
  } else {
    jfieldID fid_mn = env->GetFieldID(class_codec, "multiNodeEncode", "Z");
    jboolean multinode = env->GetIntField(obj, fid_mn);
    if (multinode) {
      jfieldID fid_ni = env->GetFieldID(class_codec, "nodeIndex", "I");
      jint node_index = env->GetIntField(obj, fid_ni) - 1;
      jfieldID fid_gdn = env->GetFieldID(class_codec, "groupDataNum", "I");
      jint group_data_num = env->GetIntField(obj, fid_gdn);
      jfieldID fid_edn = env->GetFieldID(class_codec, "encodeDataNum", "I");
      jint encode_data_num = env->GetIntField(obj, fid_edn);
      uint8_t* cur_matrix = nullptr;
      int first_group = (k - 1) % group_data_num + 1;
      // group take in charge:  data group 1 - l  =>  node l | l-1 | ... | 2 | 1
      if (node_index) {
        cur_matrix = tmp_matrix + k * (k + 1) -
                     (node_index * group_data_num + first_group);
      } else {
        cur_matrix = tmp_matrix + k * (k + 1) - first_group;
      }
      for (int i = 0, p = 0; i < m; ++i, cur_matrix += k) {
        for (int j = 0; j < encode_data_num; ++j) {
          matrix[p++] = cur_matrix[j];
        }
      }
    } else {
      memcpy(matrix, tmp_matrix + k * k, k * m);
    }
  }
  delete[] tmp_matrix;
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
      env->GetFieldID(class_codec, "encodeGftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_gftbl = (jobject)env->GetObjectField(obj, fid_gftbl);
  uint8_t* gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_gftbl);
  // get k & m
  jfieldID fid_k = env->GetFieldID(class_codec, "encodeDataNum", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_m = env->GetFieldID(class_codec, "globalNum", "I");
  jint m = env->GetIntField(obj, fid_m);

  // call isa-l init table
  ec_init_tables(k, m, encode_matrix, gftbl);
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
  // get decode_num
  jfieldID fid_dn = env->GetFieldID(class_codec, "decodeDataNum", "I");
  jint decode_num = env->GetIntField(obj, fid_dn);

  // all-1 line for xor
  uint8_t decode_matrix[decode_num];
  int i;
  for (i = 0; i < decode_num; ++i) {
    decode_matrix[i] = 1;
  }
  // call isa-l to get decode gf table
  ec_init_tables(decode_num, 1, decode_matrix, decode_gftbl);
}

JNIEXPORT void JNICALL Java_NativeCodec_initPartialDecodeTable(JNIEnv* env,
                                                               jobject obj) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get decode gf table
  jfieldID fid_pdgftbl = env->GetFieldID(class_codec, "partialDecodeGftbl",
                                         "Ljava/nio/ByteBuffer;");
  jobject jba_pdgftbl = (jobject)env->GetObjectField(obj, fid_pdgftbl);
  uint8_t* partial_decode_gftbl =
      (uint8_t*)env->GetDirectBufferAddress(jba_pdgftbl);
  // get partialDecodeNum
  jfieldID fid_pdn = env->GetFieldID(class_codec, "partialDecodeNum", "I");
  jint partial_decode_num = env->GetIntField(obj, fid_pdn);

  // all-1 line for xor
  uint8_t decode_matrix[partial_decode_num];
  int i;
  for (i = 0; i < partial_decode_num; ++i) {
    decode_matrix[i] = 1;
  }
  // call isa-l to get partial decode gf table
  ec_init_tables(partial_decode_num, 1, decode_matrix, partial_decode_gftbl);
}

JNIEXPORT void JNICALL Java_NativeCodec_encodeData(JNIEnv* env, jobject obj,
                                                   jobjectArray data_joa,
                                                   jobjectArray parity_joa) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get gf table
  jfieldID fid_gftbl =
      env->GetFieldID(class_codec, "encodeGftbl", "Ljava/nio/ByteBuffer;");
  jobject jba_gftbl = (jobject)env->GetObjectField(obj, fid_gftbl);
  uint8_t* gftbl = (uint8_t*)env->GetDirectBufferAddress(jba_gftbl);
  // get k & m & chunk_size
  jfieldID fid_k = env->GetFieldID(class_codec, "encodeDataNum", "I");
  jint k = env->GetIntField(obj, fid_k);
  jfieldID fid_m = env->GetFieldID(class_codec, "globalNum", "I");
  jint m = env->GetIntField(obj, fid_m);
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  jfieldID fid_ct = env->GetFieldID(class_codec, "codeType", "C");
  jchar code_type = env->GetIntField(obj, fid_ct);
  // get the data & parity memory space
  int i;
  jobject jba[k];
  uint8_t* data[k];
  for (i = 0; i < k; ++i) {
    jba[i] = env->GetObjectArrayElement(data_joa, i);
    data[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t* parity[k];
  for (i = 0; i < m; ++i) {
    jba[i] = env->GetObjectArrayElement(parity_joa, i);
    parity[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  // call isa-l to encode data to get global parity chunks
  ec_encode_data(chunk_size, k, m, gftbl, data, parity);

  // generate local parity chunk
  if (code_type == 'C' || code_type == 'L') {
    jfieldID fid_mn = env->GetFieldID(class_codec, "multiNodeEncode", "Z");
    jboolean multinode = env->GetIntField(obj, fid_mn);
    jfieldID fid_gdn = env->GetFieldID(class_codec, "groupDataNum", "I");
    jint group_data_num = env->GetIntField(obj, fid_gdn);
    static uint8_t* xor_gftbl = nullptr;
    static uint8_t* last_xor_gftbl = nullptr;
    static uint8_t* matrix = nullptr;
    if (!xor_gftbl) {
      int num = multinode ? k : group_data_num;
      xor_gftbl = new uint8_t[32 * num]();
      matrix = new uint8_t[num]();
      memset(xor_gftbl, 1, num);
      ec_init_tables(num, 1, matrix, xor_gftbl);
    }
    if (code_type == 'C' && multinode) {
      int pos = m;
      jba[pos] = env->GetObjectArrayElement(parity_joa, pos);
      parity[pos] = (uint8_t*)env->GetDirectBufferAddress(jba[pos]);
      ec_encode_data(chunk_size, k, 1, xor_gftbl, data, parity + m);
    } else {
      jfieldID fid_gn = env->GetFieldID(class_codec, "groupNum", "I");
      jint group_num = env->GetIntField(obj, fid_gn);
      int pos = m, offset = 0;
      for (int j = 0; j < group_num - 1; ++j, ++pos, offset += group_data_num) {
        jba[pos] = env->GetObjectArrayElement(parity_joa, pos);
        parity[pos] = (uint8_t*)env->GetDirectBufferAddress(jba[pos]);
        ec_encode_data(chunk_size, group_data_num, 1, xor_gftbl, data + offset,
                       parity + pos);
      }
      // last group
      jba[pos] = env->GetObjectArrayElement(parity_joa, pos);
      parity[pos] = (uint8_t*)env->GetDirectBufferAddress(jba[pos]);
      static int last_group = (k - 1) % group_data_num + 1;
      if (!last_xor_gftbl) {
        if (last_group != group_data_num) {
          last_xor_gftbl = new uint8_t[32 * last_group]();
          ec_init_tables(last_group, 1, matrix, last_xor_gftbl);
        } else {
          last_xor_gftbl = xor_gftbl;
        }
      }
      ec_encode_data(chunk_size, last_group, 1, last_xor_gftbl, data + offset,
                     parity + pos);
    }
  }
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
  // get decode_num & chunk_size
  jfieldID fid_dn = env->GetFieldID(class_codec, "decodeDataNum", "I");
  jint decode_num = env->GetIntField(obj, fid_dn);
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  // get the data & target memory space
  int i;
  jobject jba[decode_num];
  uint8_t* data[decode_num];
  for (i = 0; i < decode_num; ++i) {
    jba[i] = env->GetObjectArrayElement(data_joa, i);
    data[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t* target[1];
  target[0] = (uint8_t*)env->GetDirectBufferAddress(target_jba);

  // call isa-l to decode
  ec_encode_data(chunk_size, decode_num, 1, decode_gftbl, data, target);
}

JNIEXPORT void JNICALL Java_NativeCodec_partialDecodeData(JNIEnv* env,
                                                          jobject obj,
                                                          jobjectArray data_joa,
                                                          jobject target_jba) {
  // get class
  jclass class_codec = env->GetObjectClass(obj);
  // get decode gf table
  jfieldID fid_pdgftbl = env->GetFieldID(class_codec, "partialDecodeGftbl",
                                         "Ljava/nio/ByteBuffer;");
  jobject jba_pdgftbl = (jobject)env->GetObjectField(obj, fid_pdgftbl);
  uint8_t* partial_decode_gftbl =
      (uint8_t*)env->GetDirectBufferAddress(jba_pdgftbl);
  // get decode_num & chunk_size
  jfieldID fid_pdn = env->GetFieldID(class_codec, "partialDecodeNum", "I");
  jint partial_decode_num = env->GetIntField(obj, fid_pdn);
  jfieldID fid_cs = env->GetFieldID(class_codec, "chunkSize", "I");
  jint chunk_size = env->GetIntField(obj, fid_cs);
  // get the data & target memory space
  int i;
  jobject jba[partial_decode_num];
  uint8_t* data[partial_decode_num];
  for (i = 0; i < partial_decode_num; ++i) {
    jba[i] = env->GetObjectArrayElement(data_joa, i);
    data[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t* target[1];
  target[0] = (uint8_t*)env->GetDirectBufferAddress(target_jba);

  // call isa-l to decode
  ec_encode_data(chunk_size, partial_decode_num, 1, partial_decode_gftbl, data,
                 target);
}

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
  uint8_t* source[global_num];
  for (i = 0; i < global_num; ++i) {
    jba[i] = env->GetObjectArrayElement(src_joa, i);
    source[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }
  uint8_t* target[global_num];
  for (i = 0; i < global_num; ++i) {
    jba[i] = env->GetObjectArrayElement(tar_joa, i);
    target[i] = (uint8_t*)env->GetDirectBufferAddress(jba[i]);
  }

  uint8_t *data[2], *output[1];
  for (i = 0; i < global_num; ++i) {
    data[0] = source[i];
    output[0] = data[1] = target[i];
    ec_encode_data(chunk_size, 2, 1, gftbl, data, output);
  }

  flag = true;
}
