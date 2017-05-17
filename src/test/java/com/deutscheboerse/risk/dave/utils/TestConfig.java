package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SelfSignedCertificate;

import java.util.UUID;

public class TestConfig {
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    public static final int API_PORT = Integer.getInteger("api.port", 8443);
    public static final int HEALTHCHECK_PORT = Integer.getInteger("healthcheck.port", 8080);
    public static final SelfSignedCertificate API_SERVER_CERTIFICATE = SelfSignedCertificate.create("localhost");
    public static final SelfSignedCertificate API_CLIENT_CERTIFICATE = SelfSignedCertificate.create("localhost");

    private TestConfig() {
        // Empty
    }

    public static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("api", TestConfig.getApiConfig())
                .put("mongo", TestConfig.getMongoConfig())
                .put("healthCheck", TestConfig.getHealthCheckConfig());
    }

    public static JsonObject getApiConfig() {
        JsonArray sslTrustCerts = new JsonArray();
        API_CLIENT_CERTIFICATE.trustOptions().getCertPaths().forEach(certPath -> {
            Buffer certBuffer = Vertx.vertx().fileSystem().readFileBlocking(certPath);
            sslTrustCerts.add(certBuffer.toString());
        });
        Buffer pemKeyBuffer = Vertx.vertx().fileSystem().readFileBlocking(API_SERVER_CERTIFICATE.keyCertOptions().getKeyPath());
        Buffer pemCertBuffer = Vertx.vertx().fileSystem().readFileBlocking(API_SERVER_CERTIFICATE.keyCertOptions().getCertPath());
        return new JsonObject()
                .put("port", API_PORT)
                .put("sslKey", pemKeyBuffer.toString())
                .put("sslCert", pemCertBuffer.toString())
                .put("sslTrustCerts", sslTrustCerts);
    }

    public static JsonObject getMongoConfig() {
        final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
        return new JsonObject()
                .put("dbName", DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", DB_PORT, 1000));
    }

    public static JsonObject getMongoClientConfig(JsonObject mongoVerticleConfig) {
        return new JsonObject()
                .put("db_name", mongoVerticleConfig.getString("dbName"))
                .put("connection_string", mongoVerticleConfig.getString("connectionUrl"));
    }

    private static JsonObject getHealthCheckConfig() {
        return new JsonObject()
                .put("port", HEALTHCHECK_PORT);
    }

}
