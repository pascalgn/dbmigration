FROM openjdk:8-jre-alpine

ARG PROJECT_VERSION
ENV PROJECT_VERSION "${PROJECT_VERSION}"

ENV JAVA_OPTS "-Xmx4g"
ENV LOG_LEVEL "info"
ENV DATA_DIR "/home/dbmigration/data"

RUN adduser -D -g dbmigration dbmigration && mkdir "/home/dbmigration/data"

COPY "src/build/dbmigration.sh" "/home/dbmigration/"

RUN chmod +x "/home/dbmigration/dbmigration.sh" && chown -R dbmigration:dbmigration "/home/dbmigration"

USER dbmigration
WORKDIR "/home/dbmigration/data"

ENTRYPOINT [ "/home/dbmigration/dbmigration.sh" ]

COPY "target/dbmigration-${PROJECT_VERSION}-dist.jar" "/home/dbmigration/"
