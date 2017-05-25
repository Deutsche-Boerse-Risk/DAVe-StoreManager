package com.deutscheboerse.risk.dave.json;

import com.google.protobuf.MessageLite;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class GrpcJsonWrapper extends JsonObject {
    private final MessageLite grpc;

    public GrpcJsonWrapper(MessageLite grpc) {
        Objects.requireNonNull(grpc);
        this.grpc = grpc;
    }

    @Override
    public <T> T mapTo(Class<T> type) {
        return type.cast(this.grpc);
    }

    @Override
    public String getString(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLong(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getDouble(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float getFloat(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getBoolean(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject getJsonObject(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonArray getJsonArray(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getInstant(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String key, String def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(String key, Integer def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLong(String key, Long def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double getDouble(String key, Double def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Float getFloat(String key, Float def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getBoolean(String key, Boolean def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject getJsonObject(String key, JsonObject def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonArray getJsonArray(String key, JsonArray def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(String key, byte[] def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getInstant(String key, Instant def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(String key, Object def) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> fieldNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Enum value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Integer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject putNull(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, JsonObject value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, JsonArray value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Instant value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject mergeIn(JsonObject other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject mergeIn(JsonObject other, boolean deep) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject mergeIn(JsonObject other, int depth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String encode() {
        return this.grpc.toString();
    }

    @Override
    public String encodePrettily() {
        return this.grpc.toString();
    }

    @Override
    public Buffer toBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject copy() {
        return this;
    }

    @Override
    public Map<String, Object> getMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Map.Entry<String, Object>> stream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<String, Object>> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonObject clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return this.grpc.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GrpcJsonWrapper))
            return false;
        return this.grpc.equals(((GrpcJsonWrapper)o).grpc);
    }

    @Override
    public int hashCode() {
        return this.grpc.hashCode();
    }

    @Override
    public void writeToBuffer(Buffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readFromBuffer(int pos, Buffer buffer) {
        throw new UnsupportedOperationException();
    }
}
