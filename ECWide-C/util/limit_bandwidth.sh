#!/bin/bash 
dev_name=eth0

usage() {
    echo "usage:  all bandwidth<mbit/s>"
    echo "        rack bandwidth<mbit/s> [node_index]"
    echo "        clean"
    exit
}

clean() {
    # del qdisc and filter   #  > /dev/null 2>&1
    if [[ $1 == 1 ]]
    then 
        tc filter del dev ${dev_name} > /dev/null 2>&1
        echo "clean filter"
    fi
    tc qdisc del dev ${dev_name} root > /dev/null 2>&1
    echo "clean qdisc"
}

get_index_from_hostname() {
    hostname=$(cat /etc/hostname)
    node_index=${hostname: 4}
    echo "extract "$node_index
    expr $node_index "+" 10 &> /dev/null
    if [[ $? -ne 0 ]]
    then
        echo "Need to set hostname: node{%2d}"
        echo "Or you can provide node_index"
        echo ""
        usage
    fi
    if [[ ${node_index:0:1} = "0" ]]
    then
        node_index=${node_index:1}
    fi

}

get_ip_to_set() {
    mapfile ip < config/nodes.ini

    codeType=`sed '/^codeType *=/!d;s/.*=//' config/scheme.ini`
    codeType=${codeType^^}
    k=`sed '/^k *=/!d;s/.*=//' config/scheme.ini`
    globalParityNum=`sed '/^globalParityNum *=/!d;s/.*=//' config/scheme.ini`
    if [ $codeType = "CL" ]
    then
        groupDataNum=`sed '/^groupDataNum *=/!d;s/.*=//' config/scheme.ini`
        let rackNodesNum=globalParityNum+1
        let groupNum=k/groupDataNum
        if [ `expr $k % $groupDataNum` != 0 ]
        then
            let groupNum=groupNum+1
        fi
        let except_gp=groupNum+k
        if [ $node_index -gt $except_gp ]
        then
            let start_index=except_gp+1
            let end_index=except_gp+globalParityNum
        else
            let start_index=`expr $node_index - $[$node_index-1] % $rackNodesNum`
            if [ `expr $start_index + $rackNodesNum` -gt $except_gp ]
            then
                let end_index=except_gp
            else
                let end_index=start_index+rackNodesNum-1
            fi
        fi
    elif [ $codeType = "TL" ]
    then
        let rackNodesNum=globalParityNum
        if [ $node_index -gt $k ]
        then
            let start_index=k+1
            let end_index=k+globalParityNum
        else
            let start_index=`expr $node_index - $[$node_index-1] % $rackNodesNum`
            if [ `expr $start_index + $rackNodesNum` -gt $k ]
            then
                let end_index=k
            else
                let end_index=start_index+rackNodesNum-1
            fi
        fi
    else
        echo "codeType error"
        exit
    fi
    let set_num=end_index-start_index
    ip_to_set=(0)
    for (( i=$start_index, j=0; i <= $end_index; i++ ))
    do
        if [ $i -eq $node_index ]
        then
            continue
        fi
        cur_ip=$(echo ${ip[$i]} | tr '\r' ' ' | tr '\n' ' ' )    
        echo "$i, $cur_ip"
        ip_to_set[j]=$cur_ip
        let "j++"
    done
}

set_rack_bandwidth() {
    # create root qdisc 
    tc qdisc add dev ${dev_name} root handle 1: htb default 10

    # create main class and bind
    tc class add dev ${dev_name} parent 1: classid 1:1 htb rate 10000Mbit ceil 12000Mbit burst 20mb
    # create sub class, for inner-rack and cross-rack
    tc class add dev ${dev_name} parent 1:1 classid 1:10 htb rate ${bandwidth}Mbit burst 20mb
    tc class add dev ${dev_name} parent 1:1 classid 1:20 htb rate 10000Mbit ceil 12000Mbit burst 20mb

    # use Fairness Queueing
    tc qdisc add dev ${dev_name} parent 1:10 handle 10: sfq perturb 10  
    tc qdisc add dev ${dev_name} parent 1:20 handle 20: sfq perturb 10

    # create filters
    # inner-rack case, with higher priority, full rate
    get_ip_to_set
    for (( i=0; i < $set_num; i++ ))
    do
        cur_ip=${ip_to_set[i]}
        tc filter add dev ${dev_name} protocol ip parent 1:0 prio 1 u32 match ip dst ${cur_ip} flowid 1:20
    done
}

# invalid call
if [[ $# == 0 || $# -gt 3 || $# == 1  && $1 != "clean" ]]
then
    usage
fi

# main process
if [[ $1 = "all" ]]
then
    bandwidth=$2
    clean 0
    echo "set bandwidth "$bandwidth" mbit/s"
    tc qdisc add dev ${dev_name} root tbf rate ${bandwidth}mbit burst 10mb latency 50ms
    exit
elif [[ $1 = "rack" ]]
then
    if [[ $# == 3 ]]
    then 
        node_index=$3
    else
        get_index_from_hostname
    fi
    bandwidth=$2
    echo "node_index = "$node_index
    clean 1
    echo "set cross-rack bandwidth "$bandwidth" mbit/s"
    set_rack_bandwidth
    exit
elif [[ $1 = "clean" ]]
then
    # clean previous rules  
    clean 1
    exit
else
    usage
fi
