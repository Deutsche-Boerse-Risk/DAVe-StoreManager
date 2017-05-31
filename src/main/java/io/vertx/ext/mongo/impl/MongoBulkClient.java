package io.vertx.ext.mongo.impl;

import com.deutscheboerse.risk.dave.model.Model;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.model.WriteModel;

import java.util.List;

public interface MongoBulkClient extends MongoClient {

    static MongoBulkClient createShared(Vertx vertx, JsonObject config) {
        return new MongoBulkClientImpl(vertx, config, DEFAULT_POOL_NAME);
    }

    <T extends Model> MongoBulkClient bulkWrite(List<WriteModel<T>> writes, String collection, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler);
    <T extends Model> MongoBulkClient aggregate(String collection, JsonArray pipeline, Class<T> modelType, Handler<AsyncResult<List<T>>> resultHandler);
}
