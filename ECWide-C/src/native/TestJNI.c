#include <jni.h>
#include <stdio.h>
#include "TestJNI.h"

JNIEXPORT void JNICALL Java_TestJNI_sayHello(JNIEnv *env, jobject thisObject)
{
  printf("Hello World!\n");
  return;
}