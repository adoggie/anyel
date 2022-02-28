#!/usr/bin/env bash
pwd=$(cd `dirname $0`;pwd)
cd $pwd/
PYTHON=/home/tedy/anaconda3/bin/python
IP='cs28-172.16.30.28'
$PYTHON -m host-agent run "$IP"
    