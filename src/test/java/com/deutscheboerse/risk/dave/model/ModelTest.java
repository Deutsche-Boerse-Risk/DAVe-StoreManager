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
}
