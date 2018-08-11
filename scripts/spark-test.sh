#!/bin/bash
if [[ ! -z $(sudo jps -lm | grep spark) ]]
then
        echo "It seems the Spark is running"
else
        echo "Something is definitely wrong with Spark"
fi