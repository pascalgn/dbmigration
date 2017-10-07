#!/bin/sh
exec java ${JAVA_OPTS} \
    -Dorg.slf4j.simpleLogger.defaultLogLevel="${LOG_LEVEL}" \
    -jar "/home/dbmigration/dbmigration-${PROJECT_VERSION}-dist.jar" \
    "${@}"
