package io.vertx.ext.mongo.model;

public interface WriteModel<T> {
    com.mongodb.client.model.WriteModel<T> getMongoWriteModel();
}
