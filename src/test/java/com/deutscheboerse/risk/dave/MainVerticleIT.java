package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.MongoPersistenceService;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.RestSender;
import com.deutscheboerse.risk.dave.utils.RestSenderCorrectData;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(VertxUnitRunner.class)
public class MainVerticleIT extends BaseTest {
    private Vertx vertx;
    private static int ACCOUNT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1);
    private static int LIQUI_GROUP_SPLIT_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1);
    private static int POOL_MARGIN_COUNT = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1);
    private static int POSITION_REPORT_COUNT = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1);
    private static int RISK_LIMIT_UTILIZATION_COUNT = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1);

    @Before
    public void setUp() {
        this.vertx = Vertx.vertx();
    }

    private MongoClient createMongoClient(JsonObject mongoVerticleConfig) {
        return MongoClient.createShared(this.vertx, BaseTest.getMongoClientConfig(mongoVerticleConfig));
    }

    private DeploymentOptions createDeploymentOptions() {
        return new DeploymentOptions().setConfig(BaseTest.getGlobalConfig());
    }

    @Test
    public void testFullChain(TestContext context) throws IOException, InterruptedException {
        Async mainVerticleAsync = context.async();
        DeploymentOptions options = createDeploymentOptions();
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                mainVerticleAsync.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        mainVerticleAsync.awaitSuccess(30000);
        MongoClient mongoClient = this.createMongoClient(options.getConfig().getJsonObject("mongo"));
        final RestSender restSender = new RestSenderCorrectData(this.vertx);
        restSender.sendAllData(context.asyncAssertSuccess());
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(AccountMarginModel.class), ACCOUNT_MARGIN_COUNT);
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(LiquiGroupMarginModel.class), LIQUI_GROUP_MARGIN_COUNT);
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(LiquiGroupSplitMarginModel.class), LIQUI_GROUP_SPLIT_MARGIN_COUNT);
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(PoolMarginModel.class), POOL_MARGIN_COUNT);
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(PositionReportModel.class), POSITION_REPORT_COUNT);
        this.testCountInCollection(context, mongoClient, MongoPersistenceService.getCollectionName(RiskLimitUtilizationModel.class), RISK_LIMIT_UTILIZATION_COUNT);
    }

    @Test
    public void testFailedDeploymentWrongConfig(TestContext context) {
        Async mainVerticleAsync = context.async();
        DeploymentOptions options = createDeploymentOptions();
        System.setProperty("dave.configurationFile", "nonexisting");
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, ar -> {
            System.clearProperty("dave.configurationFile");
            if (ar.succeeded()) {
                context.fail(ar.cause());
            } else {
                mainVerticleAsync.complete();
            }
        });
    }

    @Test
    public void testFailedDeployment(TestContext context) {
        DeploymentOptions options = createDeploymentOptions();
        options.getConfig().getJsonObject("http", new JsonObject()).put("port", -1);
        this.vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertFailure());
    }

    private void testCountInCollection(TestContext  context, MongoClient mongoClient, String collection, long count) {
        AtomicLong currentCount = new AtomicLong();
        int tries = 0;
        while (currentCount.get() != count && tries < 60) {
            Async asyncHistoryCount = context.async();
            mongoClient.count(collection, new JsonObject(), ar -> {
                if (ar.succeeded()) {
                    currentCount.set(ar.result());
                    if (currentCount.get() == count && !asyncHistoryCount.isCompleted()) {
                        asyncHistoryCount.complete();
                    }
                } else {
                    context.fail(ar.cause());
                }
            });
            try {
                asyncHistoryCount.await(1000);
            } catch (Exception ignored) {
                asyncHistoryCount.complete();
            }
            tries++;
        }
        context.assertEquals(count, currentCount.get());
    }

    @After
    public void cleanup(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
