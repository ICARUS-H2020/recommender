#!/bin/bash

echo "Docker container has been started"

declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

echo "SHELL=/bin/bash
BASH_ENV=/container.env
*/6 * * * * /code/training.sh >> /var/log/cron.log 2>&1
#this extra line makes it a valid cron" > sheduler.txt

crontab  sheduler.txt
cron

python /code/src/recommender.py
