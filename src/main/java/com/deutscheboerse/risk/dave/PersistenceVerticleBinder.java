package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.config.MongoConfig;
import com.deutscheboerse.risk.dave.persistence.MongoPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.impl.MongoBulkClient;

import javax.inject.Singleton;
import java.io.IOException;

public class PersistenceVerticleBinder extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceVerticleBinder.class);

    @Override
    protected void configure() {
        bindMongoClient();
        bindPersistenceService();
    }

    private void bindMongoClient() {
        try {
            MongoConfig mongoConfig = (new ObjectMapper()).readValue(Vertx.currentContext().config().toString(), MongoConfig.class);

            LOG.debug("Using binder {}", mongoConfig.getGuice_binder());

            JsonObject jsonConfig = new JsonObject();

            jsonConfig.put("db_name", mongoConfig.getDbName());
            jsonConfig.put("useObjectId", true);
            jsonConfig.put("connection_string", mongoConfig.getConnectionUrl());
            MongoBulkClient mongo = MongoBulkClient.createShared(Vertx.currentContext().owner(), jsonConfig);

            bind(MongoBulkClient.class).toInstance(mongo);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void bindPersistenceService() {
        bind(PersistenceService.class).to(MongoPersistenceService.class).in(Singleton.class);
    }
}
