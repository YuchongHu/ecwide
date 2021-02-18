#!/bin/bash 

if [ $# != 3 ]
then
  echo "usage:  [stripe_num] [chunk_type] [chunk_id]"
  exit
fi

stripe_num=$1
chunk_type=$2
chunk_id=$3

find_str=$(cat config/settings.ini | grep "chunksDir")
arr=(${find_str//=/})
chunks_dir=${arr[1]}

for i in $(seq 0 `expr ${stripe_num} - 1`)
do
  cur_chunk="${chunk_type}_${i}_${chunk_id}"
  cmd="cp /tmp/stdfile ${chunks_dir}/${cur_chunk}"
  $($cmd)
done