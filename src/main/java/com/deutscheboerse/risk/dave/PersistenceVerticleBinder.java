package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.MongoPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.inject.AbstractModule;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import javax.inject.Singleton;

public class PersistenceVerticleBinder extends AbstractModule {

    private static final String DEFAULT_DB_NAME = "DAVe";
    private static final String DEFAULT_CONNECTION_URL = "mongodb://localhost:27017/?waitqueuemultiple=20000";

    @Override
    protected void configure() {
        bindMongoClient();
        bindPersistenceService();
    }

    private void bindMongoClient() {
        JsonObject globalMongoConfig = Vertx.currentContext().config();
        JsonObject mongoConfig = new JsonObject();

        mongoConfig.put("db_name", globalMongoConfig.getString("dbName", DEFAULT_DB_NAME));
        mongoConfig.put("useObjectId", true);
        mongoConfig.put("connection_string", globalMongoConfig.getString("connectionUrl", DEFAULT_CONNECTION_URL));
        MongoClient mongo = MongoClient.createShared(Vertx.currentContext().owner(), mongoConfig);

        bind(MongoClient.class).toInstance(mongo);
    }

    private void bindPersistenceService() {
        bind(PersistenceService.class).to(MongoPersistenceService.class).in(Singleton.class);
    }
}
