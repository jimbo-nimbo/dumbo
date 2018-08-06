#!/bin/bash

test_topic="up_test_topic"
test_text="Hello test"

if [ $# -eq 3 ]
    then
        zk=$2
        broker_list=$3
        echo "1)   Trying to remove topic: $test_topic..."
        $1/bin/kafka-topics.sh --zookeeper ${zk} --delete --topic ${test_topic}
        echo "2)   Trying to create topic: $test_topic..."
        $1/bin/kafka-topics.sh --create --zookeeper ${zk} --replication-factor 1 --partitions 1 --topic ${test_topic}
        echo "3)   Producing some test record..."
        echo ${test_text} | $1/bin/kafka-console-producer.sh --broker-list \
            ${broker_list} --topic ${test_topic} >/dev/null
        echo "4)   Consuming the test record..."
        out_text=$($1/bin/kafka-console-consumer.sh --bootstrap-server ${broker_list} --topic ${test_topic} \
            --from-beginning --timeout-ms 5000 2> /dev/null)
        if [ "$test_text" == "$out_text" ]
            then
                echo "Test is passed with fine colors :)"
        else
            echo "Something went terribly wrong :("
        fi
else
    echo "Usage: bash $0 <kafka-dir> <zookeeper-address> <broker-list>"
    echo "Note: Kafka server should be up and running before running this command."
    exit 1
fi