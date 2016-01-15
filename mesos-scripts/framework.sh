#!/bin/bash

if test -z "$ARANGODB_WEBUI_PORT" -o "$ARANGODB_WEBUI_PORT" == "0";  then
  ARANGODB_WEBUI_PORT="${PORT0}"
fi

if test -z "$ARANGODB_WEBUI_HOST";  then
  if test "$ARANGODB_WEBUI_USE_HOSTNAME" = "yes" -o -z "$HOST" ;  then
    ARANGODB_WEBUI=http://${HOSTNAME}:${ARANGODB_WEBUI_PORT}
  else
    ARANGODB_WEBUI=http://${HOST}:${ARANGODB_WEBUI_PORT}
  fi
else
  ARANGODB_WEBUI="http://${ARANGODB_WEBUI_HOST}:${ARANGODB_WEBUI_PORT}"
fi

# The following exports will be used internally in the libprocess startup (which does the network binding to the master)

# Apart from the webui port the framework needs another port for internal master => framework communication
export LIBPROCESS_PORT=${PORT1}
# Mesos will use this IP to communicate internally. When starting the framework in bridged mode our IP is however an internal docker IP
# We now announce ourselves as $HOST to the master which is the SLAVES IP. By facilitating port publishing in the docker executor, connecting to $PORT1 of the slave
# will be routed to the docker container :)
export LIBPROCESS_ADVERTISE_IP=${HOST}

env

echo "ARANGODB_WEBUI_PORT: $ARANGODB_WEBUI_PORT"
echo "ARANGODB_WEBUI_HOST: $ARANGODB_WEBUI_HOST"
echo "ARANGODB_WEBUI     : $ARANGODB_WEBUI"
echo "LIBPROCESS_PORT    : $LIBPROCESS_PORT"

cd /mesos
exec ./arangodb-framework "--http_port=${ARANGODB_WEBUI_PORT}" "--webui=${ARANGODB_WEBUI}" "$@"
