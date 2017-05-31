package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class StoreDocumentBsonAdapter<T extends Model> implements Bson {
    private final T model;

    public StoreDocumentBsonAdapter(T model) { this.model = model;}

    @Override
    @SuppressWarnings("unchecked")
    public <C> BsonDocument toBsonDocument(Class<C> documentClass, CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<>(model, (Codec<T>)codecRegistry.get(model.getClass()));
    }
}
