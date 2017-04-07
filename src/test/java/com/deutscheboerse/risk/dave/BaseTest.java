package com.deutscheboerse.risk.dave;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SelfSignedCertificate;

import java.util.UUID;

public class BaseTest {
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    protected static final int HTTP_PORT = Integer.getInteger("http.port", 8083);
    protected static final int HEALTHCHECK_PORT = Integer.getInteger("healthcheck.port", 8084);
    public static final SelfSignedCertificate HTTP_SERVER_CERTIFICATE = SelfSignedCertificate.create();
    static final SelfSignedCertificate HTTP_CLIENT_CERTIFICATE = SelfSignedCertificate.create();

    protected static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("http", BaseTest.getHttpConfig())
                .put("mongo", BaseTest.getMongoConfig())
                .put("healthCheck", BaseTest.getHealthCheckConfig());
    }

    static JsonObject getHttpConfig() {
        JsonArray sslTrustCerts = new JsonArray();
        HTTP_CLIENT_CERTIFICATE.trustOptions().getCertPaths().forEach(certPath -> {
            Buffer certBuffer = Vertx.vertx().fileSystem().readFileBlocking(certPath);
            sslTrustCerts.add(certBuffer.toString());
        });
        Buffer pemKeyBuffer = Vertx.vertx().fileSystem().readFileBlocking(HTTP_SERVER_CERTIFICATE.keyCertOptions().getKeyPath());
        Buffer pemCertBuffer = Vertx.vertx().fileSystem().readFileBlocking(HTTP_SERVER_CERTIFICATE.keyCertOptions().getCertPath());
        return new JsonObject()
                .put("port", HTTP_PORT)
                .put("sslKey", pemKeyBuffer.toString())
                .put("sslCert", pemCertBuffer.toString())
                .put("sslTrustCerts", sslTrustCerts);
    }

    protected static JsonObject getMongoConfig() {
        final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
        return new JsonObject()
                .put("dbName", DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", DB_PORT, 1000));
    }

    protected static JsonObject getMongoClientConfig(JsonObject mongoVerticleConfig) {
        return new JsonObject()
                .put("db_name", mongoVerticleConfig.getString("dbName"))
                .put("connection_string", mongoVerticleConfig.getString("connectionUrl"));
    }

    private static JsonObject getHealthCheckConfig() {
        return new JsonObject()
                .put("port", HEALTHCHECK_PORT);
    }

}
