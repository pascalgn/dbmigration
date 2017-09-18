FROM openjdk:8-jre-alpine

ARG PROJECT_VERSION
ENV PROJECT_VERSION "${PROJECT_VERSION}"

ENV LOG_LEVEL "debug"

RUN adduser -D -g dbmigration dbmigration

USER dbmigration
RUN mkdir "/home/dbmigration/data"

COPY "target/dbmigration-${PROJECT_VERSION}-dist.jar" "/home/dbmigration/"

CMD "java" "-Dorg.slf4j.simpleLogger.defaultLogLevel=${LOG_LEVEL}" \
    "-jar" "/home/dbmigration/dbmigration-${PROJECT_VERSION}-dist.jar" \
    "/home/dbmigration/data"
