package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class QueryParamsBsonAdapter<T extends Model> implements Bson {

    private static final Codec<Model> QUERY_PARAMS_CODEC = new QueryParamsCodec();

    private final T model;

    public QueryParamsBsonAdapter(T model) { this.model = model; }

    @Override
    public <C> BsonDocument toBsonDocument(Class<C> documentClass, CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<>(model, QUERY_PARAMS_CODEC);
    }
}
