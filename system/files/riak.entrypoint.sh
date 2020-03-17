#!/bin/sh

LOGDIR="/var/log/riak"
LOGFILE="$LOGDIR/console.log"

while [ ! -f "$LOGFILE" ] ; do sleep 5 ; done

exec tail -f "$LOGFILE"
