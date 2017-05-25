package io.vertx.ext.mongo.impl;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;
import org.bson.conversions.Bson;

import java.util.List;

public interface MongoBulkClient extends MongoClient {

    static MongoBulkClient createShared(Vertx vertx, JsonObject config) {
        return new MongoBulkClientImpl(vertx, config, DEFAULT_POOL_NAME);
    }

    static UpdateOneModel<JsonObject> newWriteModel(JsonObject query, JsonObject update, UpdateOptions options) {
        Bson bquery = new JsonObjectBsonAdapter(query);
        Bson bupdate = new JsonObjectBsonAdapter(update);
        com.mongodb.client.model.UpdateOptions updateOptions = new com.mongodb.client.model.UpdateOptions()
                .upsert(options.isUpsert());
        return new UpdateOneModel<>(bquery, bupdate, updateOptions);
    }

    MongoBulkClient bulkWrite(List<WriteModel<JsonObject>> writes, String collection, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler);
    MongoBulkClient aggregate(String collection, JsonArray pipeline, Handler<AsyncResult<List<JsonObject>>> resultHandler);
}
