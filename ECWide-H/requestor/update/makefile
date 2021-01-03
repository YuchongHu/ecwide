
LINK    = @echo Linking $@ && g++ 
GCC     = @echo Compiling $@ && g++ 
GC      = @echo Compiling $@ && gcc 
AR      = @echo Generating Static Library $@ && ar crv
FLAGS   = -std=c++11 -g -DDEBUG -W -Wall -fPIC
GCCFLAGS =
DEFINES = 
HEADER  = -I ./ /usr/local/include
LIBS    = -lmemcached -lpthread -lisal
LINKFLAGS =
BIN_PATH = ./


OBJECT_s := requestor.o common.o thread.o

#OBJECT_p := proxy.o common.o thread.o encode.o 

TARGET_s = requestor
#TARGET_p = proxy

$(TARGET_s) : $(OBJECT_s) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

#$(TARGET_p) : $(OBJECT_p) 
	$(LINK) $(FLAGS) $(LINKFLAGS) -o $@ $^ $(LIBS)

.cpp.o:
	$(GCC) -c $(HEADER) $(FLAGS) $(GCCFLAGS) -o $@ $<

.c.o:
	$(GC) -c $(HEADER) $(FLAGS) -o $@ $<

#install: $(TARGET)
#	cp $(TARGET) $(BIN_PATH)

all: $(TARGET_s)
	rm -rf *.o *.so *.a

clean:
	rm -rf $(TARGET_s) *.o *.so *.a


# ++++++++++++++++++++++++++++++++++++++
