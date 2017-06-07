package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.PoolMarginModel;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.JsonObjectBsonAdapter;
import io.vertx.ext.mongo.impl.codec.json.JsonObjectCodec;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ModelCodecTest {
    private static final CodecRegistry CODEC_REGISTRY = CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs(new JsonObjectCodec(new JsonObject())),
            CodecRegistries.fromProviders(new ModelCodecProvider()));
    private static final Codec<PoolMarginModel> CODEC = CODEC_REGISTRY.get(PoolMarginModel.class);

    @Test(expected = UnsupportedOperationException.class)
    public void testEncode(TestContext context) {
        PoolMarginModel model = DataHelper.getLastModelFromFile(
                DataHelper.POOL_MARGIN_FOLDER, 1, PoolMarginModel::buildFromJson);

        BsonDocument expectedDocument = new JsonObjectBsonAdapter(model.getMongoStoreDocument())
                .toBsonDocument(null, CODEC_REGISTRY);

        BsonDocument document = new BsonDocument();
        CODEC.encode(new BsonDocumentWriter(document), model, EncoderContext.builder().build());

        context.assertEquals(expectedDocument, document);
    }

    @Test
    public void testDecode(TestContext context) {
        JsonObject json = DataHelper.getLastJsonFromFile(DataHelper.POOL_MARGIN_FOLDER, 1)
                .orElseThrow(RuntimeException::new);

        BsonDocument bson = new JsonObjectBsonAdapter(json).toBsonDocument(null, CODEC_REGISTRY);
        PoolMarginModel poolMarginModel = CODEC.decode(new BsonDocumentReader(bson), DecoderContext.builder().build());
        PoolMarginModel expected = PoolMarginModel.buildFromJson(json);

        context.assertEquals(expected.toGrpc(), poolMarginModel.toGrpc());
    }

    @Test
    public void testGetEncoderClass(TestContext context) {
        context.assertEquals(PoolMarginModel.class, CODEC.getEncoderClass());
    }
}
