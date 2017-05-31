package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.Model;
import com.deutscheboerse.risk.dave.model.RiskLimitUtilizationModel;
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
public class QueryParamsCodecTest {
    private static final CodecRegistry CODEC_REGISTRY = CodecRegistries.fromCodecs(new JsonObjectCodec(new JsonObject()));
    private static final Codec<Model> CODEC = new QueryParamsCodec();

    @Test
    public void testEncode(TestContext context) {
        RiskLimitUtilizationModel model = DataHelper.getLastModelFromFile(
                DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1, RiskLimitUtilizationModel::buildFromJson);

        BsonDocument expectedDocument = new JsonObjectBsonAdapter(model.getMongoQueryParams())
                .toBsonDocument(null, CODEC_REGISTRY);

        BsonDocument document = new BsonDocument();
        CODEC.encode(new BsonDocumentWriter(document), model, EncoderContext.builder().build());

        context.assertEquals(expectedDocument, document);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDecode() {
        CODEC.decode(new BsonDocumentReader(new BsonDocument()), DecoderContext.builder().build());
    }

    @Test
    public void testGetEncoderClass(TestContext context) {
        context.assertEquals(Model.class, CODEC.getEncoderClass());
    }
}
