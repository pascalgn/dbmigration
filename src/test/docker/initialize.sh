#!/bin/bash

set -e

SERVER_OUT=/var/log/sqlserver.out
TIMEOUT=90

/opt/mssql/bin/sqlservr &>${SERVER_OUT} &

function server_ready() {
    grep -q -F 'Recovery is complete.' ${SERVER_OUT}
}

# Wait until the server is ready
for (( i=0; i<${TIMEOUT}; i++ )); do
    if server_ready; then
        break
    fi
    sleep 1
done

if ! server_ready; then
    echo "Server is not ready after ${TIMEOUT} seconds!" >&2
    echo 'Log output:' >&2
    tail -n 3 ${SERVER_OUT} >&2
    exit 1
fi

echo 'Server is up'

# Execute the initialization SQL script
cat /tmp/setup/initialize.sql | /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "${SA_PASSWORD}"
