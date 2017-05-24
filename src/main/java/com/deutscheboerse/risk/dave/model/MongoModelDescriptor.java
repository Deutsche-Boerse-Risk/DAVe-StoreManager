package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;

import java.util.Collection;

public interface MongoModelDescriptor {
    String getCollectionName();

    default JsonObject getIndex() {
        JsonObject uniqueIndex = new JsonObject();
        getCommonFields().forEach(field -> uniqueIndex.put(field, 1));
        return uniqueIndex;
    }

    Collection<String> getCommonFields();
    Collection<String> getSnapshotFields();
}
