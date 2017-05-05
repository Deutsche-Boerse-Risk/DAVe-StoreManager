package io.vertx.ext.mongo.impl;

import com.mongodb.client.model.WriteModel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;

import java.util.List;

public interface MongoBulkClient extends MongoClient {

    static MongoBulkClient createShared(Vertx vertx, JsonObject config) {
        return new MongoBulkClientImpl(vertx, config, DEFAULT_POOL_NAME);
    }

    MongoBulkClient queueBulkUpdate(List<WriteModel<JsonObject>> writes, JsonObject query, JsonObject update, UpdateOptions options);
    MongoBulkClient bulkWrite(List<WriteModel<JsonObject>> writes, String collection, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler);
}
