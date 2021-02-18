target: java_compile native_codec_header native_codec native_msg_queue_header native_msg_queue
java_compile: src/*.java
	javac -d classes $^

native_codec_header: src/native/NativeCodec.h

src/native/NativeCodec.h: src/NativeCodec.java
	javac -d classes/ -h src/native/ $^

native_codec: lib/libcodec.so

lib/libcodec.so: src/native/NativeCodec.cc /usr/lib/libisal.so
	@mkdir -p lib
	g++ -fPIC -std=c++11 -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o $@ $^

native_msg_queue_header: src/native/NativeMessageQueue.h

src/native/NativeMessageQueue.h: src/NativeMessageQueue.java
	javac -d classes/ -h src/native/ $^

native_msg_queue: lib/libmsg_queue.so

lib/libmsg_queue.so: src/native/NativeMessageQueue.cc
	@mkdir -p lib
	g++ -fPIC -std=c++11 -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o $@ $^


native_test: src/native/TestJNI.c
	gcc -fPIC -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o lib/libhello.so $^

clean:
	rm -f classes/* lib/*
