package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.function.Function;

public class ModelCodec<T extends Model> implements Codec<T> {

    private static final Codec<JsonObject> JSON_OBJECT_CODEC = new JsonObjectCodec(new JsonObject());

    private final Function<JsonObject, T> modelBuilder;
    private final Class<T> encoderClass;

    ModelCodec(Function<JsonObject, T> modelBuilder, Class<T> encoderClass) {
        this.modelBuilder = modelBuilder;
        this.encoderClass = encoderClass;
    }

    @Override
    public void encode(BsonWriter writer, T value, EncoderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T decode(BsonReader reader, DecoderContext ctx) {
        return modelBuilder.apply(JSON_OBJECT_CODEC.decode(reader, ctx));
    }

    @Override
    public Class<T> getEncoderClass() {
        return this.encoderClass;
    }
}
