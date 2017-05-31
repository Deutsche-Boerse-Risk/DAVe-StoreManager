package com.deutscheboerse.risk.dave.mongo.model;

import com.deutscheboerse.risk.dave.mongo.codec.QueryModelBsonAdapter;
import com.deutscheboerse.risk.dave.mongo.codec.UpdateModelBsonAdapter;
import com.deutscheboerse.risk.dave.model.Model;
import com.mongodb.client.model.UpdateOptions;
import io.vertx.ext.mongo.model.WriteModel;

public class UpdateOneModel<T extends Model> implements WriteModel<T> {
    private static final UpdateOptions UPSERT_OPTIONS    = new UpdateOptions().upsert(true);
    private static final UpdateOptions NO_UPSERT_OPTIONS = new UpdateOptions().upsert(false);

    private final T model;
    private final io.vertx.ext.mongo.UpdateOptions updateOptions;

    public UpdateOneModel(T model, io.vertx.ext.mongo.UpdateOptions updateOptions) {
        this.model = model;
        this.updateOptions = updateOptions;
    }

    @Override
    public com.mongodb.client.model.WriteModel<T> getMongoWriteModel() {
        return new com.mongodb.client.model.UpdateOneModel<>(
                new QueryModelBsonAdapter<>(model),
                new UpdateModelBsonAdapter<>(model),
                updateOptions.isUpsert() ? UPSERT_OPTIONS : NO_UPSERT_OPTIONS);
    }
}
