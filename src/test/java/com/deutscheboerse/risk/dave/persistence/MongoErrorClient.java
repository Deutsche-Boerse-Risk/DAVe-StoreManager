package com.deutscheboerse.risk.dave.persistence;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.*;

import java.util.ArrayList;
import java.util.List;

public class MongoErrorClient implements io.vertx.ext.mongo.MongoClient {

    private final JsonObject config;

    MongoErrorClient(JsonObject config) {
        this.config = config;
    }

    @Override
    public MongoClient save(String collection, JsonObject document, Handler<AsyncResult<String>> resultHandler) {
        return this.process("save", "", resultHandler);
    }

    @Override
    public MongoClient saveWithOptions(String collection, JsonObject document, WriteOption writeOption, Handler<AsyncResult<String>> resultHandler) {
        return this.process("saveWithOptions", "", resultHandler);
    }

    @Override
    public MongoClient insert(String collection, JsonObject document, Handler<AsyncResult<String>> resultHandler) {
        return this.process("insert", "", resultHandler);
    }

    @Override
    public MongoClient insertWithOptions(String collection, JsonObject document, WriteOption writeOption, Handler<AsyncResult<String>> resultHandler) {
        return this.process("insertWithOptions", "", resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient update(String collection, JsonObject query, JsonObject update, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("update", null, resultHandler);
    }

    @Override
    public MongoClient updateCollection(String collection, JsonObject query, JsonObject update, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        return this.process("updateCollection", new MongoClientUpdateResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient updateWithOptions(String collection, JsonObject query, JsonObject update, UpdateOptions options, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("updateWithOptions", null, resultHandler);
    }

    @Override
    public MongoClient updateCollectionWithOptions(String collection, JsonObject query, JsonObject update, UpdateOptions options, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        return this.process("updateCollectionWithOptions", new MongoClientUpdateResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient replace(String collection, JsonObject query, JsonObject replace, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("replace", null, resultHandler);
    }

    @Override
    public MongoClient replaceDocuments(String collection, JsonObject query, JsonObject replace, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        return this.process("replaceDocuments", new MongoClientUpdateResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient replaceWithOptions(String collection, JsonObject query, JsonObject replace, UpdateOptions options, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("replaceWithOptions", null, resultHandler);
    }

    @Override
    public MongoClient replaceDocumentsWithOptions(String collection, JsonObject query, JsonObject replace, UpdateOptions options, Handler<AsyncResult<MongoClientUpdateResult>> resultHandler) {
        return this.process("replaceDocumentsWithOptions", new MongoClientUpdateResult(), resultHandler);
    }

    @Override
    public MongoClient find(String collection, JsonObject query, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        return this.process("find", new ArrayList<>(), resultHandler);
    }

    @Override
    public MongoClient findBatch(String collection, JsonObject query, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findBatch", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findWithOptions(String collection, JsonObject query, FindOptions options, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        return this.process("findWithOptions", new ArrayList<>(), resultHandler);
    }

    @Override
    public MongoClient findBatchWithOptions(String collection, JsonObject query, FindOptions options, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findBatchWithOptions", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOne(String collection, JsonObject query, JsonObject fields, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOne", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndUpdate(String collection, JsonObject query, JsonObject update, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndUpdate", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndUpdateWithOptions(String collection, JsonObject query, JsonObject update, FindOptions findOptions, UpdateOptions updateOptions, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndUpdateWithOptions", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndReplace(String collection, JsonObject query, JsonObject replace, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndReplace", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndReplaceWithOptions(String collection, JsonObject query, JsonObject replace, FindOptions findOptions, UpdateOptions updateOptions, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndReplaceWithOptions", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndDelete(String collection, JsonObject query, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndDelete", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient findOneAndDeleteWithOptions(String collection, JsonObject query, FindOptions findOptions, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("findOneAndDeleteWithOptions", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient count(String collection, JsonObject query, Handler<AsyncResult<Long>> resultHandler) {
        return this.process("count", 0L, resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient remove(String collection, JsonObject query, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("remove", null, resultHandler);
    }

    @Override
    public MongoClient removeDocuments(String collection, JsonObject query, Handler<AsyncResult<MongoClientDeleteResult>> resultHandler) {
        return this.process("removeDocuments", new MongoClientDeleteResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient removeWithOptions(String collection, JsonObject query, WriteOption writeOption, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("removeWithOptions", null, resultHandler);
    }

    @Override
    public MongoClient removeDocumentsWithOptions(String collection, JsonObject query, WriteOption writeOption, Handler<AsyncResult<MongoClientDeleteResult>> resultHandler) {
        return this.process("removeDocumentsWithOptions", new MongoClientDeleteResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient removeOne(String collection, JsonObject query, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("removeOne", null, resultHandler);
    }

    @Override
    public MongoClient removeDocument(String collection, JsonObject query, Handler<AsyncResult<MongoClientDeleteResult>> resultHandler) {
        return this.process("removeDocument", new MongoClientDeleteResult(), resultHandler);
    }

    @Override
    @Deprecated
    public MongoClient removeOneWithOptions(String collection, JsonObject query, WriteOption writeOption, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("removeOneWithOptions", null, resultHandler);
    }

    @Override
    public MongoClient removeDocumentWithOptions(String collection, JsonObject query, WriteOption writeOption, Handler<AsyncResult<MongoClientDeleteResult>> resultHandler) {
        return this.process("removeDocumentWithOptions", new MongoClientDeleteResult(), resultHandler);
    }

    @Override
    public MongoClient createCollection(String collectionName, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("createCollection", null, resultHandler);
    }

    @Override
    public MongoClient getCollections(Handler<AsyncResult<List<String>>> resultHandler) {
        return this.process("getCollections", new ArrayList<>(), resultHandler);
    }

    @Override
    public MongoClient dropCollection(String collection, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("dropCollection", null, resultHandler);
    }

    @Override
    public MongoClient createIndex(String collection, JsonObject key, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("createIndex", null, resultHandler);
    }

    @Override
    public MongoClient createIndexWithOptions(String collection, JsonObject key, IndexOptions options, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("createIndexWithOptions", null, resultHandler);
    }

    @Override
    public MongoClient listIndexes(String collection, Handler<AsyncResult<JsonArray>> resultHandler) {
        return this.process("listIndexes", new JsonArray(), resultHandler);
    }

    @Override
    public MongoClient dropIndex(String collection, String indexName, Handler<AsyncResult<Void>> resultHandler) {
        return this.process("dropIndex", null, resultHandler);
    }

    @Override
    public MongoClient runCommand(String commandName, JsonObject command, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("runCommand", new JsonObject(), resultHandler);
    }

    @Override
    public MongoClient distinct(String collection, String fieldName, String resultClassname, Handler<AsyncResult<JsonArray>> resultHandler) {
        return this.process("distinct", new JsonArray(), resultHandler);
    }

    @Override
    public MongoClient distinctBatch(String collection, String fieldName, String resultClassname, Handler<AsyncResult<JsonObject>> resultHandler) {
        return this.process("distinctBatch", new JsonObject(), resultHandler);
    }

    @Override
    public void close() {

    }

    private <T> MongoClient process(String functionName, T defaultResult, Handler<AsyncResult<T>> resultHandler) {
        if (this.config.getJsonArray("functionsToFail", new JsonArray()).contains(functionName)) {
            resultHandler.handle(Future.failedFuture("Error in " + functionName));
        } else {
            resultHandler.handle(Future.succeededFuture(defaultResult));
        }
        return this;
    }
}
