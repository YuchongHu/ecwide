target: java_compile native_codec cpp_encode

java_compile: src/*.java
	javac -d classes $^

native_test: src/native/TestJNI.c
	gcc -fPIC -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o lib/libhello.so $^

native_codec: src/native/NativeCodec.cc /usr/lib/libisal.so
	g++ -fPIC -std=c++11 -shared -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -o lib/libcodec.so $^


# for c++ encode

#  dir base
HEADER_DIR := src/encode/include
SRC_DIR := src/encode
OBJ_DIR := build/obj
BIN_DIR := build/bin
CXX := g++
CXXFLAGS := -march=native -o3 -std=c++11 -pthread -I$(HEADER_DIR) #-g -fstack-protector -fstack-protector-all


$(OBJ_DIR)/%.o : $(SRC_DIR)/%.cc
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@



cpp_encode: node_main local_encode

node_main: $(OBJ_DIR)/node_main.o $(OBJ_DIR)/tcp_client.o $(OBJ_DIR)/tcp_server.o $(OBJ_DIR)/tcp_node.o $(OBJ_DIR)/encode_worker.o $(OBJ_DIR)/utils.o /usr/local/lib/libsockpp.so /usr/lib/libisal.so
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $(BIN_DIR)/$@

local_encode: $(OBJ_DIR)/local_encode.o $(OBJ_DIR)/utils.o /usr/lib/libisal.so /usr/local/lib/libsockpp.so
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $(BIN_DIR)/$@

clean:
	rm -f $(BIN_DIR)/* $(OBJ_DIR)/*
