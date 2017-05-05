package io.vertx.ext.mongo.impl;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec;
import io.vertx.ext.mongo.impl.config.MongoClientOptionsParser;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.Function;

public class MongoBulkClientImpl extends MongoClientImpl implements MongoBulkClient {

    private final Vertx vertx;
    private final MongoDatabase db;

    public MongoBulkClientImpl(Vertx vertx, JsonObject config, String dataSourceName) {
        super(vertx, config, dataSourceName);

        MongoClientOptionsParser parser = new MongoClientOptionsParser(config);
        this.vertx = vertx;
        this.db = this.mongo.getDatabase(parser.database());
    }

    @Override
    public MongoBulkClient queueBulkUpdate(List<WriteModel<JsonObject>> writes, JsonObject query, JsonObject update, UpdateOptions options) {
        Bson bquery = new JsonObjectBsonAdapter(query);
        Bson bupdate = new JsonObjectBsonAdapter(update);
        com.mongodb.client.model.UpdateOptions updateOptions = new com.mongodb.client.model.UpdateOptions().upsert(options.isUpsert());
        writes.add(new UpdateOneModel<>(bquery, bupdate, updateOptions));
        return this;
    }

    @Override
    public MongoBulkClient bulkWrite(List<WriteModel<JsonObject>> writes, String collection, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        if (!writes.isEmpty()) {
            this.db.getCollection(collection, JsonObject.class).bulkWrite(writes, toMongoClientUpdateResult(resultHandler));
        } else {
            resultHandler.handle(Future.succeededFuture());
        }
        return this;
    }

    private SingleResultCallback<BulkWriteResult> toMongoClientUpdateResult(Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        return convertCallback(resultHandler, result -> {
            if (result.wasAcknowledged()) {
                return convertToMongoClientUpdateResult(result.getMatchedCount(), new BsonInt32(0), result.getModifiedCount());
            } else {
                return null;
            }
        });
    }

    private MongoClientUpdateResult convertToMongoClientUpdateResult(long docMatched, BsonValue upsertId, long docModified) {
        JsonObject jsonUpsertId;
        if (upsertId != null) {
            JsonObjectCodec jsonObjectCodec = new JsonObjectCodec(new JsonObject());

            BsonDocument upsertIdDocument = new BsonDocument();
            upsertIdDocument.append(MongoClientUpdateResult.ID_FIELD, upsertId);

            BsonDocumentReader bsonDocumentReader = new BsonDocumentReader(upsertIdDocument);
            jsonUpsertId = jsonObjectCodec.decode(bsonDocumentReader, DecoderContext.builder().build());
        } else {
            jsonUpsertId = null;
        }

        return new MongoClientUpdateResult(docMatched, jsonUpsertId, docModified);
    }

    private <T, R> SingleResultCallback<T> convertCallback(Handler<AsyncResult<R>> resultHandler, Function<T, R> converter) {
        Context context = vertx.getOrCreateContext();
        return (result, error) -> {
            context.runOnContext(v -> {
                if (error != null) {
                    resultHandler.handle(Future.failedFuture(error));
                } else {
                    resultHandler.handle(Future.succeededFuture(converter.apply(result)));
                }
            });
        };
    }
}
