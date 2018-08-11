#!/bin/bash
if [[ ! -z $(sudo jps | grep HMaster) ]]
then
        echo "halle"
else
        echo "hmaster run nist"
fi