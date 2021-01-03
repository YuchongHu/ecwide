#! /bin/bash

rm repair.txt
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
make clean
make all
./proxy