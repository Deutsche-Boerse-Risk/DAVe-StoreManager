package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class QueryParamsCodec implements Codec<Model> {

    private static final Codec<JsonObject> JSON_OBJECT_CODEC = new JsonObjectCodec(new JsonObject());

    @Override
    public void encode(BsonWriter writer, Model value, EncoderContext ctx) {
        JSON_OBJECT_CODEC.encode(writer, value.getMongoQueryParams(), ctx);
    }

    @Override
    public Model decode(BsonReader reader, DecoderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<Model> getEncoderClass() {
        return Model.class;
    }
}