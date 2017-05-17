package com.deutscheboerse.risk.dave.model;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ModelTest {

    @Test(expected = IllegalArgumentException.class)
    public void testCorruptedJson(TestContext context) {
        new AccountMarginModel(new JsonObject());
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedAccountMarginModel(TestContext context) {
        new AccountMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedLiquiGroupMarginModel(TestContext context) {
        new LiquiGroupMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedLiquiGroupSplitMarginModel(TestContext context) {
        new LiquiGroupSplitMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedPoolMarginModel(TestContext context) {
        new PoolMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedPositionReportModel(TestContext context) {
        new PositionReportModel(new JsonObject().put("grpc", new byte[]{0}));
    }

    @Test(expected = RuntimeException.class)
    public void testRiskLimitUtilizationModel(TestContext context) {
        new RiskLimitUtilizationModel(new JsonObject().put("grpc", new byte[]{0}));
    }
}
