#!/usr/bin/env bash

# Copy the DAVe binaries
cp -r -v ./target/dave-store-manager-1.0-SNAPSHOT/dave-store-manager-1.0-SNAPSHOT ./dockerfile/dave-store-manager-1.0-SNAPSHOT

# Delete the prefilled
rm -r ./dockerfile/dave-store-manager-1.0-SNAPSHOT/etc/storemanager.conf ./dockerfile/dave-store-manager-1.0-SNAPSHOT/etc/*.keystore ./dockerfile/dave-store-manager-1.0-SNAPSHOT/etc/*.truststore ./dockerfile/dave-store-manager-1.0-SNAPSHOT/etc/truststore
docker login -e="$DOCKER_EMAIL" -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
docker build -t dbgdave/dave-store-manager:${CIRCLE_SHA1} ./dockerfile/
docker tag -f dbgdave/dave-store-manager${CIRCLE_SHA1} docker.io/dbgdave/dave-store-manager:${CIRCLE_SHA1}
docker push dbgdave/dave-store-manager${CIRCLE_SHA1}
docker tag -f dbgdave/dave-store-manager${CIRCLE_SHA1} docker.io/dbgdave/dave-store-manager:${CIRCLE_BRANCH}
docker push dbgdave/dave-store-manager${CIRCLE_BRANCH}
