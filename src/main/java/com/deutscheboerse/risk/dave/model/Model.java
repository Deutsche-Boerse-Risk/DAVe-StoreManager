package com.deutscheboerse.risk.dave.model;

import com.google.protobuf.MessageLite;
import io.vertx.core.json.JsonObject;

public interface Model<T extends MessageLite> {

    default JsonObject toJson() {
        return new JsonObject().put("grpc", this.toGrpc().toByteArray());
    }

    T toGrpc();

    default void verifyJson(JsonObject json) {
        if (!json.containsKey("grpc")) {
            throw new IllegalArgumentException("Expected grpc field");
        }
    }

    JsonObject getMongoQueryParams();
    JsonObject getMongoStoreDocument();
}
