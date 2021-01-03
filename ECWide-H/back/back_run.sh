#! /bin/bash

ps -ef|grep "./back"|cut -c 9-15|xargs kill -9
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
gcc -std=c++11 back.cpp -o back -lmemcached -lpthread
./back $1 $2 #./back RACK Thread_num