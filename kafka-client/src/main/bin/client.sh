#!/bin/bash

# executed script file
PRG="$0"

SIGNNAME="fileextract/.*/kafka-client"

# is or not connect file
while [ -h "$PRG"  ]; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`/"$link"
    fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

PROJECT_DIR=`cd "$PRGDIR/.." >/dev/null; pwd`
echo PROJECT_DIR=$PROJECT_DIR

echo `cat $PROJECT_DIR/bin/allocations`

function status() {
    for i in `cat $PROJECT_DIR/bin/allocations | awk '{print $1}'`;
    do
        echo "$i: status ...";
        ssh -n $i "ps aux | grep \"$SIGNNAME\" | grep -v grep "
    done;
}

function stop() {
    for i in `cat $PROJECT_DIR/bin/allocations | awk '{print $1}'`;
    do
        echo "$i: stop ...";
        ssh -n $i "ps aux | grep \"$SIGNNAME\" | grep -v grep | awk '{print \$2}' | xargs kill -9"
    done;
}

function deployOne() {
    echo "deployOne"
    echo $*
    HOST_IP=$1
    echo $HOST_IP
    shift
    echo $@
    for i in "$@" 
    do 
        ssh -n ${HOST_IP} "mkdir -p ~/run-work/fileextract/"$i"/"
        scp -r $PROJECT_DIR/../kafka-client ${HOST_IP}:~/run-work/fileextract/"$i"/
        ssh -n ${HOST_IP} "cd ~/run-work/fileextract/"$i"/kafka-client/ ; sed -i 's/^partitions=.*$/partitions="$i"/g' conf/kafka.properties"
    done
}

function deploy() {
    echo "deploy"
    cat $PROJECT_DIR/bin/allocations | while read i
    do
        echo "$i: deploy ...";
        deployOne $i
    done

}

function updateOne() {
    echo "updateOne"
    echo $*
    HOST_IP="$1"
    shift
    echo $@
    for i in "$@" 
    do 
        scp -r $PROJECT_DIR/lib ${HOST_IP}:~/run-work/fileextract/"$i"/kafka-client
    done
}

function update() {
    echo "update"
    echo "copy jar to cluster";
    # scp jars
    cat $PROJECT_DIR/bin/allocations | while read i
    do
        echo "$i: update ...";
        updateOne $i
    done;
}

function startOne() {
    echo "startOne"
    echo $*
    HOST_IP="$1"
    shift
    for i in "$@" 
    do 
        ssh -n $HOST_IP "cd ~/run-work/fileextract/"$i"/kafka-client/ ; ./bin/ctl.sh start fileExtractConsumerSingle > /dev/null"
        sleep 5
    done
}

function start() {
    echo "start"
    cat $PROJECT_DIR/bin/allocations | while read i
    do
        echo "$i: start ..."
        startOne $i
    done
}

function deldata() {
    for i in `cat $PROJECT_DIR/bin/allocations | awk '{print $1}'`;
    do
        echo "$i: status ...";
        ssh $i "rm -r ~/run-work/fileextract/"
    done;
}

case $1 in
    "status")
        status
        ;;
    "stop")
        stop
        ;;
    "deploy")
        deploy
        ;;
    "update")
        update
        ;;
    "start")
        start
        ;;
    "deldata")
        deldata
        ;;
    "*")
        echo "error parameters"
        ;;
esac
