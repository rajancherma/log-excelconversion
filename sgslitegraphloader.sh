#!/bin/bash

#reading the properties file
oldIFS="$IFS"
IFS=$'\n' arr=($(</home/verizon/Downloads/test.properties))
IFS="$oldIFS"

set echo off

#path to store the csv files after splitting the input file ending with *complete.csv extension
csvpath=/home/verizon

if [ "x$LOADER_HOME" = "x" ]; then
    LOADER_HOME="`pwd`"
fi

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -n "$JAVA_HOME" ]; then
    # Why we can't have nice things: Solaris combines x86 and x86_64
    # installations in the same tree, using an unconventional path for the
    # 64bit JVM.  Since we prefer 64bit, search the alternate path first,
    # (see https://issues.apache.org/jira/browse/CASSANDRA-4638).
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables. > /dev/stderr
    exit 1;
fi

# Xmx needs to be set so that it is big enough to cache all the vertexes in the run
export JVM_OPTS="$JVM_OPTS -Xmx10g"

export LOADER_CLASSPATH="$LOADER_HOME/dse-graph-loader-*-uberjar.jar*"

# add additional jars to the classpath if the lib directory exists
if [ -d "$LOADER_HOME"/lib ]; then
  export LOADER_CLASSPATH="$LOADER_CLASSPATH:lib/*"
fi

export JVM_OPTS="$JVM_OPTS -cp $LOADER_CLASSPATH"
export JVM_OPTS="$JVM_OPTS -Xmx10g"

# Uncomment to enable debugging
#JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1414"


inotifywait -m /home/verizon/Downloads/watcher -e create |
    while read path action file; do
        if [[ $file == *complete.csv ]]
        then
        withValue=$csvpath/${file/complete.csv/}_withvalue.csv
        withOutValue=$csvpath/${file/complete.csv/}_withoutvalue.csv
        egrep "SGsLite-LOCATION-UPDATE|SGsLite-UPLINK-UNITDATA" $path$file | cut -d "," -f1,2,3,6,8,9,15,16,18 > $withValue   
        egrep "SGsLite-IMSI-DETACH-INDICATION|SGsLite-DOWNLINK-UNITDATA" $path$file | cut -d "," -f1,2,2,3,6,15,16,18 > $withOutValue
           if [[ $withValue == *withvalue.csv ]] 
              then
              arrlocal=("${arr[@]}")
              arrlocal+=(-filename $withValue)
              arrlocal+=(/home/verizon/Downloads/Groovy_files/sgslite_with_val.groovy)
              "$JAVA" $JVM_OPTS com.datastax.dsegraphloader.cli.Executable ${arrlocal[@]} &                       
           fi
           if [[ $withOutValue == *withoutvalue.csv ]]
               then
                arrlocal=("${arr[@]}")
                arrlocal+=(-filename $withOutValue)
                arrlocal+=(/home/verizon/Downloads/Groovy_files/sgslite_without_val.groovy)
                "$JAVA" $JVM_OPTS com.datastax.dsegraphloader.cli.Executable ${arrlocal[@]} &
           fi
        fi         
    done 
