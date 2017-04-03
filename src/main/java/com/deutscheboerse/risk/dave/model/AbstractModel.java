package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public void validate() {
        validateMissingFields();
        validateUnknownFields();
    }

    protected void validateMissingFields() {
        List<String> missingFields = Stream.of(getHeader(), getKeys(), getNonKeys())
                .flatMap(Collection::stream)
                .filter(field -> !containsKey(field))
                .collect(Collectors.toList());
        if (!missingFields.isEmpty()) {
            throw new IllegalArgumentException("Missing fields in model: " + missingFields.toString());
        }
    }

    private void validateUnknownFields() {
        List<String> unknownFields = fieldNames()
                .stream()
                .filter(field -> !getHeader().contains(field))
                .filter(field -> !getKeys().contains(field))
                .filter(field -> !getNonKeys().contains(field))
                .collect(Collectors.toList());
        if (!unknownFields.isEmpty()) {
            throw new IllegalArgumentException("Unknown field in model: " + unknownFields.toString());
        }
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
