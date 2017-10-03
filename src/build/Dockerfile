FROM openjdk:8-jre-alpine

ARG PROJECT_VERSION
ENV PROJECT_VERSION "${PROJECT_VERSION}"

ENV LOG_LEVEL "debug"
ENV DATA_DIR "/home/dbmigration/data"

RUN adduser -D -g dbmigration dbmigration && mkdir "/home/dbmigration/data"

COPY "src/build/dbmigration.sh" "target/dbmigration-${PROJECT_VERSION}-dist.jar" "/home/dbmigration/"

RUN chmod +x "/home/dbmigration/dbmigration.sh" && chown -R dbmigration:dbmigration "/home/dbmigration"

USER dbmigration

CMD [ "/home/dbmigration/dbmigration.sh" ]
