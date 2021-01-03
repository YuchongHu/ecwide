#! /bin/bash

# for ((i=0;i<56;i++));
# do
#     var=`printf "node%03d" $i`
#     `ssh-copy-id -i ~/.ssh/id_rsa.pub node@$var`
#     #echo $var
# done

for ((i=0;i<10;i++));
do
    var=`printf "node-%02d" $i`
    `ssh-copy-id -i ~/.ssh/id_rsa.pub node@$var`
    #echo $var
done