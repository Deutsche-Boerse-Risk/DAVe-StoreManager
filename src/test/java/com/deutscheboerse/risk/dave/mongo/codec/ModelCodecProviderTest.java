package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ModelCodecProviderTest {
    private static final CodecRegistry CODEC_REGISTRY = CodecRegistries.fromCodecs();
    private static final CodecProvider CODEC_PROVIDER = new ModelCodecProvider();

    @Test
    public void testAccountMarginModel(TestContext context) {
        context.assertEquals(AccountMarginModel.class,
                CODEC_PROVIDER.get(AccountMarginModel.class, CODEC_REGISTRY).getEncoderClass());
    }
    @Test
    public void testLiquiGroupMarginModel(TestContext context) {
        context.assertEquals(LiquiGroupMarginModel.class,
                CODEC_PROVIDER.get(LiquiGroupMarginModel.class, CODEC_REGISTRY).getEncoderClass());
    }

    @Test
    public void testLiquiGroupSplitMarginModel(TestContext context) {
        context.assertEquals(LiquiGroupSplitMarginModel.class,
                CODEC_PROVIDER.get(LiquiGroupSplitMarginModel.class, CODEC_REGISTRY).getEncoderClass());
    }

    @Test
    public void testPoolMarginModel(TestContext context) {
        context.assertEquals(PoolMarginModel.class,
                CODEC_PROVIDER.get(PoolMarginModel.class, CODEC_REGISTRY).getEncoderClass());
    }

    @Test
    public void testPositionReportModel(TestContext context) {
        context.assertEquals(PositionReportModel.class,
                CODEC_PROVIDER.get(PositionReportModel.class, CODEC_REGISTRY).getEncoderClass());
    }

    @Test
    public void testRiskLimitUtilizationModel(TestContext context) {
        context.assertEquals(RiskLimitUtilizationModel.class,
                CODEC_PROVIDER.get(RiskLimitUtilizationModel.class, CODEC_REGISTRY).getEncoderClass());
    }

    @Test
    public void testUnknownModel(TestContext context) {
        context.assertNull(CODEC_PROVIDER.get(Object.class, CODEC_REGISTRY));
    }
}
