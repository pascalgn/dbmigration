#!/bin/sh
exec java -Dorg.slf4j.simpleLogger.defaultLogLevel="${LOG_LEVEL}" \
    -jar "/home/dbmigration/dbmigration-${PROJECT_VERSION}-dist.jar" \
    "${DATA_DIR}"
