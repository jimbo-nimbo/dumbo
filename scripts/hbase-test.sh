#!/bin/bash
if [[ ! -z $(sudo jps | grep HMaster) ]]
then
        echo "It seems the HBase is running"
else
        echo "Something is definitely wrong with HBase"
fi