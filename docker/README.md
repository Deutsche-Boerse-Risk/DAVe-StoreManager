# DAVe Docker image

**DAVe** docker image allows DAVe to be executed in Docker / Kubernetes. It contains an entrypoint which will take case of the configuration based on environment variables. The different options are described below.

## Examples

To run DAVe in Docker, you have to pass the environment variables to the `docker run` command.

`docker run -ti -P -e DAVE_HTTP_ENABLE=1 -e DAVE_HTTP_COMPRESSION=1 -e DAVE_HTTP_AUTH=1 -e DAVE_HTTP_SSL_SERVER_PUBLIC_KEY="$webCrt" -e DAVE_HTTP_
SSL_SERVER_PRIVATE_KEY="$webKey" -e DAVE_DB_HOSTNAME=172.17.0.1 -e DAVE_DB_PORT=27017 -e DAVE_DB_NAME=DAVe scholzj/dave:latest`

To actually use the application, you have to point to a host running the MongoDB database.

## Options

### Logging

Allows to configure logging parameters. Supported log levels are `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` and `ALL`.

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_LOG_LEVEL` | Logging level which should be used | `INFO` |


### Database

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_DB_NAME` | Name of the database which will be used | |
| `DAVE_DB_URL` | Connection URL for Mongo database. Can be used for more complex database configuration. When set, options `DAVE_DB_HOSTNAME`, `DAVE_DB_PORT`, `DAVE_DB_USERNAME` and `DAVE_DB_PASSWORD` will be ignored. | `mongodb://user:pass@mongo1:27017,mongo2:27017/?replicaSet=test` |
| `DAVE_DB_HOSTNAME` | Hostname of the Mongo server | `mongo.mydomain.tld` |
| `DAVE_DB_PORT` | Port where the Mongo database is listening | `27017` |
| `DAVE_DB_USERNAME` | Username to connect into Mongo | |
| `DAVE_DB_PASSWORD` | Password to connect into Mongo | |

### HTTP Server

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_HTTP_ENABLE` | Enable the HTTP server | `1` |
| `DAVE_HTTP_COMPRESSION` | Enable compression of HTTP responses | `1` |
| `DAVE_HTTP_CORS` | Enable Cross-origin resource sharing | `1` |
| `DAVE_HTTP_CORS_ORIGIN` | Set the Cross-origin resource sharing origin host | `myhost.mydomain.tld` |
| `DAVE_HTTP_CSRF` | Enable the Cross-site request forgery handler | `1` |
| `DAVE_HTTP_CSRF_SECRET` | Configures the domain secret used to generate CSFR tokens | `61d77a85-276b-476a-8810-f8408b5cfa19` |
| `DAVE_HTTP_SSL_SERVER_PUBLIC_KEY` | Public key of the HTTP server in CRT format | |
| `DAVE_HTTP_SSL_SERVER_PRIVATE_KEY` | Private key of the HTTP server in PEM format | |
| `DAVE_HTTP_SSL_TRUSTED_CA` | List of trusted CA for SSL client authentication | |
| `DAVE_HTTP_SSL_CLIENT_AUTH` | MAke SSL Client Authentication required | `1` |
| `DAVE_HTTP_AUTH` | Enable authentication | `1` |
| `DAVE_HTTP_AUTH_SALT` | Set the salt for password hashing | `sdf8hdgss3_a` |
| `DAVE_HTTP_AUTH_LINK_SSL` | Validate the username against the CN of the SSL client certificate | `1` |
| `DAVE_HTTP_JWT_BASE64_KEYSTORE` | Base64 encoded JCEKS keystore required by JWT authentication provider | |
| `DAVE_HTTP_JWT_KEYSTORE_PASSWORD` | Password to the JWT keystore | `123456` |
| `DAVE_HTTP_JWT_TOKEN_EXPIRATION` | Expiration time (in minutes) when the JWT token expires | `60` minutes |
