package com.deutscheboerse.risk.dave;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SelfSignedCertificate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.UUID;

public class BaseTest {
    private static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    protected static final int HTTP_PORT = Integer.getInteger("http.port", 8083);
    public static final SelfSignedCertificate HTTP_SERVER_CERTIFICATE = SelfSignedCertificate.create();
    public static final SelfSignedCertificate HTTP_CLIENT_CERTIFICATE = SelfSignedCertificate.create();

    protected static JsonObject getGlobalConfig() {
        return new JsonObject()
                .put("http", BaseTest.getHttpConfig())
                .put("mongo", BaseTest.getMongoConfig());
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
}
