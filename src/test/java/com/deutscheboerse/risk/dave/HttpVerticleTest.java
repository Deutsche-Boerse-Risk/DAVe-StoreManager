package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.persistence.CountdownPersistenceService;
import com.deutscheboerse.risk.dave.persistence.ErrorPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.restapi.StoreApi;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.RestSenderIgnoreError;
import com.deutscheboerse.risk.dave.utils.RestSenderRegular;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class HttpVerticleTest {
    private final TestAppender testAppender = TestAppender.getAppender(StoreApi.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;

    @Before
    public void setUp() throws IOException {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
    }

    @Test
    public void testStoreAccountMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreAccountMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreLiquiGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testLiquiGroupMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testStoreLiquiSplitGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testLiquiGroupSplitMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testStorePoolMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testPoolMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testStorePositionReport(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testPositionReportError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testStoreRiskLimitUtilization(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testRiskLimitUtilizationError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStoreError(context, msgCount, new RestSenderIgnoreError(this.vertx)::sendRiskLimitUtilizationData);
    }

    private void testStore(TestContext context, int msgCount, Consumer<Handler<AsyncResult<Void>>> sender) {
        Async async = context.async(msgCount);

        CountdownPersistenceService persistenceService = new CountdownPersistenceService(async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getHttpConfig());
        vertx.deployVerticle(HttpVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());

        sender.accept(context.asyncAssertSuccess());

        async.awaitSuccess(30000);

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    private void testStoreError(TestContext context, int msgCount, Consumer<Handler<AsyncResult<Void>>> sender) throws InterruptedException {
        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getHttpConfig());
        vertx.deployVerticle(HttpVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
        Async async = context.async();
        sender.accept(ar -> {
            if (ar.succeeded()) {
                async.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        async.awaitSuccess();
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        ILoggingEvent logMessage = testAppender.getLastMessage(Level.ERROR);
        testAppender.stop();
        rootLogger.addAppender(stdout);

        context.assertEquals(Level.ERROR, logMessage.getLevel());
        context.assertTrue(logMessage.getFormattedMessage().contains("Failed to store the document"));
        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @After
    public void cleanup(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
    }
}
