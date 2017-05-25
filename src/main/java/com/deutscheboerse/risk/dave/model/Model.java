package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.json.GrpcJsonWrapper;
import com.google.protobuf.MessageLite;
import io.vertx.core.json.JsonObject;

public interface Model<T extends MessageLite> {

    default JsonObject toJson() {
        return new GrpcJsonWrapper(toGrpc());
    }

    T toGrpc();

    default void verifyJson(JsonObject json) {
        if (!(json instanceof GrpcJsonWrapper)) {
            throw new IllegalArgumentException("Expected grpc wrapper");
        }
    }

    JsonObject getMongoQueryParams();
    JsonObject getMongoStoreDocument();
}
