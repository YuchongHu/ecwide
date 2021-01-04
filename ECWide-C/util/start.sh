#!/bin/bash --login
if [ $# -ne 1 ]
then
  echo "node index is required!"
  exit
fi

source source /etc/profile
java SocketNode $1