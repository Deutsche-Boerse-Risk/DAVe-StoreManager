package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.LiquiGroupSplitMarginModel;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.JsonObjectBsonAdapter;
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class StoreDocumentBsonAdapterTest {
    private static final CodecRegistry CODEC_REGISTRY = CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs(new JsonObjectCodec(new JsonObject())),
            CodecRegistries.fromProviders(new ModelCodecProvider()));

    @Test
    public void testToBsonDocument(TestContext context) {
        LiquiGroupSplitMarginModel model = DataHelper.getLastModelFromFile(
                DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1, LiquiGroupSplitMarginModel::buildFromJson);

        Bson bsonAdapter = new StoreDocumentBsonAdapter<>(model);

        BsonDocument expectedDocument = new JsonObjectBsonAdapter(model.getMongoStoreDocument())
                .toBsonDocument(null, CODEC_REGISTRY);

        context.assertEquals(expectedDocument, bsonAdapter.toBsonDocument(null, CODEC_REGISTRY));
    }
}
