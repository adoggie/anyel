#!/usr/bin/env bash

pwd=$(cd `dirname $0`;pwd)
cd $pwd/../
PYTHON=/home/tedy/anaconda3/bin/python
nohup $PYTHON -m elabs.utils.mx-broker run_spbb 'tcp://*:31010' 'tcp://*:31011' > /dev/null 2>&1 &

# */5 * * * * flock -xn /tmp/start-mx-service.lock -c 'bash /home/scott/RapidMarket/scripts/start-mx-service.sh > /dev/null 2>&1 &’
# */5 * * * * bash /home/scott/RapidMarket/scripts/start-mx-service.sh