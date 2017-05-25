package com.deutscheboerse.risk.dave.persistence;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.impl.MongoBulkClient;
import org.bson.BsonDocument;

import java.util.Collections;
import java.util.List;

public class MongoDuplicateKeyErrorClient extends MongoErrorClient {

    MongoDuplicateKeyErrorClient() {
        super(new JsonObject());
    }

    @Override
    public MongoBulkClient bulkWrite(List<WriteModel<JsonObject>> writes, String collection, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        Throwable throwable = new MongoBulkWriteException(
                BulkWriteResult.unacknowledged(),
                Collections.singletonList(new BulkWriteError(11000, "duplicate key", new BsonDocument(), 0)),
                null, new ServerAddress());
        resultHandler.handle(Future.failedFuture(throwable));
        return this;
    }

    @Override
    public void close() {

    }
}
