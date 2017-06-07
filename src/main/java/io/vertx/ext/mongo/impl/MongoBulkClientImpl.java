package io.vertx.ext.mongo.impl;

import com.deutscheboerse.risk.dave.model.Model;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.AggregateIterable;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.config.MongoBulkClientOptionsParser;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class MongoBulkClientImpl extends MongoClientImpl implements MongoBulkClient {

    private final Vertx vertx;
    private final MongoDatabase db;

    MongoBulkClientImpl(Vertx vertx, JsonObject config, String dataSourceName) {
        super(vertx, config, dataSourceName);

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        this.vertx = vertx;
        this.mongo = MongoClients.create(parser.settings());
        this.db = this.mongo.getDatabase(parser.database());
    }

    @Override
    public <T extends Model> MongoBulkClient aggregate(String collection, JsonArray pipeline, Class<T> modelType, Handler<AsyncResult<List<T>>> resultHandler) {
        requireNonNull(pipeline, "pipeline cannot be null");
        requireNonNull(resultHandler, "resultHandler cannot be null");

        List<Bson> bsonPipeline = new ArrayList<>();
        pipeline.forEach(json -> bsonPipeline.add(new JsonObjectBsonAdapter((JsonObject)json)));

        AggregateIterable<T> view = this.db.getCollection(collection, modelType).aggregate(bsonPipeline, modelType);
        List<T> results = new ArrayList<>();
        view.into(results, convertBulkCallback(resultHandler, Function.identity()));
        return this;
    }

    private <T, R> SingleResultCallback<T> convertBulkCallback(Handler<AsyncResult<R>> resultHandler, Function<T, R> converter) {
        Context context = vertx.getOrCreateContext();
        return (result, error) ->
            context.runOnContext(v -> {
                if (error != null) {
                    resultHandler.handle(Future.failedFuture(error));
                } else {
                    resultHandler.handle(Future.succeededFuture(converter.apply(result)));
                }
            });
    }
}
