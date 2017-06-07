package io.vertx.ext.mongo.impl;

import com.deutscheboerse.risk.dave.model.AccountMarginModel;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class MongoBulkClientImplTest {
    @Test
    public void testAggregateFailure(TestContext context) {
        JsonObject config = TestConfig.getMongoConfig();
        JsonObject mongoConfig = TestConfig.getMongoClientConfig(config);

        MongoBulkClient mongoClient = MongoBulkClient.createShared(Vertx.vertx(), mongoConfig);
        mongoClient.aggregate(DataHelper.ACCOUNT_MARGIN_FOLDER,
                new JsonArray().add(new JsonObject().put("unknown_command", 10)),
                AccountMarginModel.class, context.asyncAssertFailure());
    }
}
