package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractModel extends JsonObject {

    AbstractModel() {
    }

    AbstractModel(JsonObject json) {
        this.mergeIn(json);
    }

    public Map<String, Class> getHeaderDescriptor() {
        Map<String, Class<?>> header = new LinkedHashMap<>();
        header.put("snapshotID", Integer.class);
        header.put("businessDate", Integer.class);
        header.put("timestamp", Long.class);
        return Collections.unmodifiableMap(header);
    }

    public JsonObject toJson() {
        return new JsonObject(this.getMap());
    }

    public abstract Map<String, Class> getKeysDescriptor();
    public abstract Map<String, Class> getNonKeysDescriptor();

    public Collection<String> getHeader() {
        return getHeaderDescriptor().keySet();
    }

    public Collection<String> getKeys() {
        return getKeysDescriptor().keySet();
    }

    public Collection<String> getNonKeys() {
        return getNonKeysDescriptor().keySet();
    }

}
