#!/usr/bin/env bash

APP_NAME=nytimes-batch-processor
JOB_NAME=${APP_NAME}
SCHEDULER_SERVICE_NAME=scheduler-joshlong
DB_SERVICE_NAME=covid19-db

cf d -f ${APP_NAME}

cf s | grep ${DB_SERVICE_NAME} || cf cs elephantsql turtle ${DB_SERVICE_NAME}
cf bs ${APP_NAME} ${DB_SERVICE_NAME}

cf s | grep ${SCHEDULER_SERVICE_NAME} || cf cs scheduler-for-pcf standard ${SCHEDULER_SERVICE_NAME}
cf bs ${APP_NAME} ${SCHEDULER_SERVICE_NAME}

cf push -b java_buildpack -u none --no-route --no-start -p target/${APP_NAME}.jar ${APP_NAME}

cf set-env ${APP_NAME} COVIDDB_PW   ${COVIDDB_PW}
cf set-env ${APP_NAME} COVIDDB_USER ${COVIDDB_USER}
cf set-env ${APP_NAME} COVIDDB_NAME ${COVIDDB_NAME}
cf set-env ${APP_NAME} COVIDDB_HOST ${COVIDDB_HOST}

cf set-env ${APP_NAME} SPRING_PROFILES_ACTIVE cloud
cf set-env ${APP_NAME} JBP_CONFIG_OPEN_JDK_JRE '{ jre: { version: 11.+}}'
cf restart ${APP_NAME}

cf schedule-job ${JOB_NAME} "*/15 * ? * *"
cf run-job ${JOB_NAME}
