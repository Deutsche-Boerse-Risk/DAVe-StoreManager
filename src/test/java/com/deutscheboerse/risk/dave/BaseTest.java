package com.deutscheboerse.risk.dave;

import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class BaseTest {
    protected static final int DB_PORT =  Integer.getInteger("mongodb.port", 27017);
    protected static final int HTTP_PORT = Integer.getInteger("http.port", 8083);


    protected static JsonObject getGlobalConfig() {
        JsonObject globalConfig = new JsonObject()
                .put("http", BaseTest.getHttpConfig())
                .put("mongo", BaseTest.getMongoConfig());
        return globalConfig;
    }

    protected static JsonObject getHttpConfig() {
        JsonObject brokerConfig = new JsonObject()
                .put("port", HTTP_PORT);
        return brokerConfig;
    }

    protected static JsonObject getMongoConfig() {
        final String DB_NAME = "DAVe-Test" + UUID.randomUUID().getLeastSignificantBits();
        JsonObject mongoConfig = new JsonObject()
                .put("dbName", DB_NAME)
                .put("connectionUrl", String.format("mongodb://localhost:%s/?waitqueuemultiple=%d", DB_PORT, 1000));
        return mongoConfig;
    }

    protected static JsonObject getMongoClientConfig(JsonObject mongoVerticleConfig) {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", mongoVerticleConfig.getString("dbName"))
                .put("connection_string", mongoVerticleConfig.getString("connectionUrl"));
        return mongoConfig;
    }
}
