[![CircleCI](https://circleci.com/gh/Deutsche-Boerse-Risk/DAVe-StoreManager.svg?style=svg)](https://circleci.com/gh/Deutsche-Boerse-Risk/DAVe-StoreManager)
[![Build Status](https://travis-ci.org/Deutsche-Boerse-Risk/DAVe-StoreManager.svg?branch=master)](https://travis-ci.org/Deutsche-Boerse-Risk/DAVe-StoreManager)
[![Coverage Status](https://coveralls.io/repos/github/Deutsche-Boerse-Risk/DAVe-StoreManager/badge.svg?branch=master)](https://coveralls.io/github/Deutsche-Boerse-Risk/DAVe-StoreManager?branch=master)
[![SonarQube](https://sonarqube.com/api/badges/gate?key=com.deutscheboerse.risk:dave-store-manager)](https://sonarqube.com/dashboard/index/com.deutscheboerse.risk:dave-store-manager)

## Build

```
mvn clean package
```

The shippable artifact will be built in `target/dave-store-manager-VERSION` directory.

## Configure

Configuration is stored in `storemanager.conf` file in Hocon format. Configuration is split into several sections:

### Api

The `api` section contains the configuration of the DAVe-StoreManager where the margining data will be persisted.


| Option | Explanation | Example |
|--------|-------------|---------|
| `port` | Port where the DAVe-StoreManager is listening to HTTPS connections | 8443 |
| `sslKey` | Private key of the DAVe-MarginLoader | |
| `sslCert` | Public key of the DAVe-MarginLoader | |
| `sslTrustCerts` | List of trusted certification authorities | |
| `sslRequireClientAuth` | Sets TLS client authentication as required | |

### Mongo

| Option | Explanation | Example |
|--------|-------------|---------|
| `dbName` | Name of the database which will be used | `DAVe` |
| `connectionUrl` | Connection URL for Mongo database. | `mongodb://localhost:27017/?waitqueuemultiple=20000` |

### Health Check

The `healthCheck` section contains configuration where the REST API for checking the health/readiness status of the
microservice will be published.

| Option | Explanation | Example |
|--------|-------------|---------|
| `port` | Port of the HTTP server hosting REST API | 8080 |

The REST API provides two endpoints for checking the state using HTTP GET method:
- /healthz
- /readiness

## Run

Use script `start.sh` to start the application.

### Docker image to run standalone API
[DAVe-API Docker image](docker)


