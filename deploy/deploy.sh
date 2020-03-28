#!/usr/bin/env bash

APP_NAME=feed-ingest-job
JOB_NAME=${APP_NAME}
SCHEDULER_SERVICE_NAME=scheduler-joshlong

cf d -f ${APP_NAME}

cf s | grep ${SCHEDULER_SERVICE_NAME} || cf cs scheduler-for-pcf standard ${SCHEDULER_SERVICE_NAME}
cf bs ${APP_NAME} ${SCHEDULER_SERVICE_NAME}

cf push -b java_buildpack -u none --no-route --no-start -p target/${APP_NAME}.jar ${APP_NAME}
cf set-env ${APP_NAME} SPRING_PROFILES_ACTIVE cloud
cf set-env ${APP_NAME} JBP_CONFIG_OPEN_JDK_JRE '{ jre: { version: 11.+}}'
cf restart ${APP_NAME}