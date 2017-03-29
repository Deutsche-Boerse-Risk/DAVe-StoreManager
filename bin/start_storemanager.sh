#!/bin/bash

#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8000,suspend=n"

WHEREAMI=`dirname "${0}"`
if [ -z "${STOREMANAGER_ROOT}" ]; then
    export STOREMANAGER_ROOT=`cd "${WHEREAMI}/../" && pwd`
fi

STOREMANAGER_LIB=${STOREMANAGER_ROOT}/lib
STOREMANAGER_ETC=${STOREMANAGER_ROOT}/etc
export STOREMANAGER_LOG=${STOREMANAGER_ROOT}/log

java ${DEBUG} \
     -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
     -Dlogback.configurationFile=${STOREMANAGER_ETC}/logback.xml \
     -Ddave.configurationFile=${STOREMANAGER_ETC}/storemanager.conf \
     -jar ${STOREMANAGER_LIB}/dave-store-manager-1.0-SNAPSHOT-fat.jar
