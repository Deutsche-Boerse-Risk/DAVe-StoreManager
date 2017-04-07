#!/usr/bin/env bash

# Copy the DAVe binaries
cp -r -v ./target/dave-store-manager-1.0-SNAPSHOT/dave-store-manager-1.0-SNAPSHOT ./docker/dave-store-manager-1.0-SNAPSHOT

sed -i 's/sslKey.*/sslKey\ =\ \"\"/' ./docker/dave-store-manager-1.0-SNAPSHOT/etc/storemanager.conf
sed -i 's/sslCert.*/sslKey\ =\ \"\"/' ./docker/dave-store-manager-1.0-SNAPSHOT/etc/storemanager.conf

docker login -e="$DOCKER_EMAIL" -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
docker build -t dbgdave/dave-store-manager:${CIRCLE_SHA1} ./docker/
docker tag -f dbgdave/dave-store-manager:${CIRCLE_SHA1} docker.io/dbgdave/dave-store-manager:${CIRCLE_SHA1}
docker push dbgdave/dave-store-manager:${CIRCLE_SHA1}
docker tag -f dbgdave/dave-store-manager:${CIRCLE_SHA1} docker.io/dbgdave/dave-store-manager:${CIRCLE_BRANCH}
docker push dbgdave/dave-store-manager:${CIRCLE_BRANCH}
