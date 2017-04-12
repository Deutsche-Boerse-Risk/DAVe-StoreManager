# DAVe Store Manager Docker image

**DAVe Store Manager** docker image allows DAVe Store Manager to be executed in Docker / Kubernetes. The different options, provided via environment variables, are described below.

## Examples

To run DAVe Store Manager  in Docker, you have to pass the environment variables to the `docker run` command.

`docker run -ti -P -e API_SSL_CERT="$webCrt" -e API_SSL_KEY="$webKey" dbgdave/dave-store-manager:latest`

To actually use the application, you have to point to a host running the MongoDB database.

## Options

### General

| Option | Explanation | Example |
|--------|-------------|---------|
| `JAVA_OPTS` | JVM options | `-Xmx=512m` |


### Logging

Allows to configure logging parameters. Supported log levels are `off`, `error`, `warn`, `info`, `debug`, `trace` and `all`.

| Option | Explanation | Example |
|--------|-------------|---------|
| `LOG_LEVEL` | Logging level which should be used | `info` |


### Mongo Database

| Option | Explanation | Example |
|--------|-------------|---------|
| `MONGO_DB` | Name of the database which will be used | `DAVe` |
| `MONGO_CONNECTION_URL` | Connection URL for Mongo database. | `mongodb://localhost:27017/?waitqueuemultiple=20000` |

### HTTP Server

| Option | Explanation | Example |
|--------|-------------|---------|
| `API_SSL_KEY` | Private key of the HTTP server in PEM format |  |
| `API_SSL_CERT` | Public key of the HTTP server in CRT format |  |
| `API_SSL_TRUST_CERTS` | List of trusted CA for SSL client authentication |  |
| `API_SSL_REQUIRE_CLIENT_AUTH` | Make SSL Client Authentication required | `true` |
