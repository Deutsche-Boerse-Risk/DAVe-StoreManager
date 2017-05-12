package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.*;
import com.deutscheboerse.risk.dave.restapi.StoreApi;
import com.deutscheboerse.risk.dave.utils.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
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
import java.util.function.Function;

@RunWith(VertxUnitRunner.class)
public class ApiVerticleTest {
    private final TestAppender testAppender = TestAppender.getAppender(StoreApi.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;

    @Before
    public void setUp() throws IOException {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
    }

    // Account Margin

    @Test
    public void testStoreAccountMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreAccountMarginError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testQueryAccountMargin(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER, AccountMarginModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER, AccountMarginModel::new);
    }

    // LiquiGroup Margin

    @Test
    public void testStoreLiquiGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testStoreLiquiGroupMarginError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testQueryLiquiGroupMargin(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER, LiquiGroupMarginModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER, LiquiGroupMarginModel::new);
    }

    // LiquiGroupSplit Margin

    @Test
    public void testStoreLiquiSplitGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testStoreLiquiGroupSplitMarginError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testQueryLiquiGroupSplitMargin(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER,
//                LiquiGroupSplitMarginModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER,
//                LiquiGroupSplitMarginModel::new);
    }

    // Pool Margin

    @Test
    public void testStorePoolMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testStorePoolMarginError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testQueryPoolMargin(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER, PoolMarginModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER, PoolMarginModel::new);
    }

    // Position Report

    @Test
    public void testStorePositionReport(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testStorePositionReportError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testQueryPositionReport(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER, PositionReportModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER, PositionReportModel::new);
    }

    // Risk Limit Utilization

    @Test
    public void testStoreRiskLimitUtilization(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStore(context, msgCount, new GrpcSenderRegular(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testStoreRiskLimitUtilizationError(TestContext context) throws InterruptedException {
        this.testStoreError(context, 2, "Failed to store the document", new GrpcSenderRegularIgnoreError(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testQueryRiskLimitUtilization(TestContext context) {
//        this.testQueryCompleteHistoryUrl(context, QUERY_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER,
//                RiskLimitUtilizationModel::new);
//        this.testQueryCompleteLatestUrl(context, QUERY_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER,
//                RiskLimitUtilizationModel::new);
    }

    @Test
    public void testSSLClientAuthentication(TestContext context) {
//        EchoPersistenceService persistenceService = new EchoPersistenceService();
//        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);
//
//        this.deployApiVerticle(context, new JsonObject().put("sslRequireClientAuth", true));
//
//        final Async asyncWithoutCert = context.async();
//        HttpClientOptions httpClientOptions = createSslOptions();
//        vertx.createHttpClient(httpClientOptions).get(TestConfig.API_PORT, "localhost", QUERY_POSITION_REPORT_API + "/latest", res ->
//            context.fail("Connected to HTTPS with required client authentication without certificate!")
//        ).exceptionHandler(res -> asyncWithoutCert.complete()).end();
//        asyncWithoutCert.awaitSuccess(30000);
//
//        final Async asyncWithCert = context.async();
//        httpClientOptions.setPemKeyCertOptions(TestConfig.API_CLIENT_CERTIFICATE.keyCertOptions());
//        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", QUERY_POSITION_REPORT_API + "/latest", res -> {
//            context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
//            asyncWithCert.complete();
//        });
//
//        asyncWithCert.awaitSuccess(30000);
//        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testQueryIncompleteUrl(TestContext context) {
//        JsonObject queryParams = new JsonObject()
//                .put("clearer", "CLEARER")
//                .put("member", "MEMBER")
//                .put("account", "ACCOUNT");
//
//        JsonArray expectedResult = new JsonArray().add(new JsonObject()
//                .put("model", "PositionReportModel")
//                .put("requestType", "LATEST")
//                .mergeIn(queryParams));
//        EchoPersistenceService persistenceService = new EchoPersistenceService();
//        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);
//
//        this.deployApiVerticle(context);
//
//        final Async async = context.async();
//        this.createSslClient().getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(),
//                asyncAssertEquals(context, async, expectedResult));
//
//        async.awaitSuccess(300000);
//        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testQueryBadDataType(TestContext context) {
//        JsonObject queryParams = new JsonObject()
//                .put("clearer", "CLEARER")
//                .put("member", "MEMBER")
//                .put("contractYear", 1234.5d);
//
//        this.deployApiVerticle(context);
//
//        final Async async = context.async();
//        this.createSslClient().getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
//            context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), res.statusCode());
//            async.complete();
//        });
//
//        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryUnknownParameter(TestContext context) {
//        JsonObject queryParams = new JsonObject()
//                .put("clearer", "CLEARER")
//                .put("member", "MEMBER")
//                .put("foo", 2016.2);
//
//        this.deployApiVerticle(context);
//
//        final Async async = context.async();
//        this.createSslClient().getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
//            context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), res.statusCode());
//            async.complete();
//        });
//
//        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryUnknownModel(TestContext context) {
//        JsonObject queryParams = new JsonObject()
//                .put("clearer", "CLEARER")
//                .put("member", "MEMBER");
//
//        this.deployApiVerticle(context);
//
//        final Async async = context.async();
//        this.createSslClient().getNow(TestConfig.API_PORT, "localhost", new URIBuilder(String.format("%s/query/unknown/latest", ApiVerticle.API_PREFIX)).addParams(queryParams).build(), res -> {
//            context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), res.statusCode());
//            async.complete();
//        });
//
//        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryError(TestContext context) throws InterruptedException {
//        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
//        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);
//
//        this.deployApiVerticle(context);
//
//        JsonObject queryParams = new JsonObject()
//                .put("clearer", "CLEARER")
//                .put("member", "MEMBER")
//                .put("account", "ACCOUNT");
//        final Async async = context.async();
//        this.createSslClient().getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
//            context.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), res.statusCode());
//            async.complete();
//        });
//        async.awaitSuccess(30000);
//
//        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    private void testStore(TestContext context, int msgCount, Consumer<Handler<AsyncResult<Void>>> sender) {
        Async async = context.async(msgCount);

        CountdownPersistenceService persistenceService = new CountdownPersistenceService(async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        this.deployApiVerticle(context);

        sender.accept(context.asyncAssertSuccess());
        async.awaitSuccess(30000);
        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    private void testStoreError(TestContext context, int msgCount, String errorMessage, Consumer<Handler<AsyncResult<Void>>> sender) throws InterruptedException {
        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();

        this.deployApiVerticle(context);

        sender.accept(context.asyncAssertSuccess());
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, errorMessage);
        testAppender.stop();
        rootLogger.addAppender(stdout);

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

//    private void testQueryCompleteHistoryUrl(TestContext context, String uri, String folderName,
//            Function<JsonObject, ? extends AbstractModel> modelFactory) {
//        this.testQueryCompleteUrl(context, uri+"/history", folderName, RequestType.HISTORY, modelFactory);
//    }
//
//    private void testQueryCompleteLatestUrl(TestContext context, String uri, String folderName,
//            Function<JsonObject, ? extends AbstractModel> modelFactory) {
//        this.testQueryCompleteUrl(context, uri+"/latest", folderName, RequestType.LATEST, modelFactory);
//    }
//
//    private void testQueryCompleteUrl(TestContext context, String uri, String folderName, RequestType requestType,
//                Function<JsonObject, ? extends AbstractModel> modelFactory) {
//        EchoPersistenceService persistenceService = new EchoPersistenceService();
//        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);
//
//        JsonObject queryParams = DataHelper.getQueryParams(DataHelper.getLastModelFromFile(folderName, 1, modelFactory));
//
//        final String modelClassName = modelFactory.apply(new JsonObject()).getClass().getSimpleName();
//
//        JsonArray expectedResult = new JsonArray().add(new JsonObject()
//                .put("model", modelClassName)
//                .put("requestType", requestType)
//                .mergeIn(queryParams)
//        );
//        this.deployApiVerticle(context);
//
//        final Async async = context.async();
//        this.createSslClient()
//                .getNow(TestConfig.API_PORT, "localhost", new URIBuilder(uri).addParams(queryParams).build(),
//                        asyncAssertEquals(context, async, expectedResult));
//
//        async.awaitSuccess(30000);
//        ProxyHelper.unregisterService(serviceMessageConsumer);
//    }

    private static Handler<HttpClientResponse> asyncAssertEquals(TestContext context, Async async, JsonArray expectedResult) {
        return res -> {
            context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
            res.bodyHandler(body -> {
                JsonArray bd = body.toJsonArray();
                context.assertEquals(expectedResult, bd);
                async.complete();
            });
        };
    }

    private static HttpClientOptions createSslOptions() {
        return new HttpClientOptions()
                .setSsl(true)
                .setVerifyHost(false)
                .setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
    }

    private HttpClient createSslClient() {
        return this.vertx.createHttpClient(createSslOptions());
    }

    private void deployApiVerticle(TestContext context) {
        this.deployApiVerticle(context, new JsonObject());
    }

    private void deployApiVerticle(TestContext context, JsonObject options) {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig().mergeIn(options));
        Async asyncGrpcDeploy = context.async();
        vertx.deployVerticle(GrpcVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(
                ar -> asyncGrpcDeploy.complete()
        ));
        asyncGrpcDeploy.awaitSuccess();
    }

    @After
    public void cleanup(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
        testAppender.clear();
    }
}
