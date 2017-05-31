package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class QueryModelBsonAdapter<T extends Model> implements Bson {

    private static final Codec<Model> MODEL_QUERY_CODEC = new ModelCodec<Model>(
            json -> {
                throw new UnsupportedOperationException();
            }, Model.class) {

        @Override
        public void encode(BsonWriter writer, Model value, EncoderContext ctx) {
            JSON_OBJECT_CODEC.encode(writer, value.getMongoQueryParams(), ctx);
        }
    };

    private final T model;

    public QueryModelBsonAdapter(T model) { this.model = model; }

    @Override
    public <C> BsonDocument toBsonDocument(Class<C> documentClass, CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<>(model, MODEL_QUERY_CODEC);
    }
}
