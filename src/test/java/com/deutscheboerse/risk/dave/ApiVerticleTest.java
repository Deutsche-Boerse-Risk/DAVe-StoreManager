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
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
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

@RunWith(VertxUnitRunner.class)
public class ApiVerticleTest {
    private static final String QUERY_ACCOUNT_MARGIN_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.ACCOUNT_MARGIN_REQUEST_PARAMETER);
    private static final String QUERY_LIQUI_GROUP_MARGIN_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.LIQUI_GROUP_MARGIN_REQUEST_PARAMETER);
    private static final String QUERY_LIQUI_GROUP_SPLIT_MARGIN_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER);
    private static final String QUERY_POOL_MARGIN_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.POOL_MARGIN_REQUEST_PARAMETER);
    private static final String QUERY_POSITION_REPORT_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.POSITION_REPORT_REQUEST_PARAMETER);
    private static final String QUERY_RISK_LIMIT_UTILIZATION_API = String.format("%s/query/%s", ApiVerticle.API_PREFIX, ApiVerticle.RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER);

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
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreAccountMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreAccountMarginMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testStoreAccountMarginUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.ACCOUNT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendAccountMarginData);
    }

    @Test
    public void testQueryAccountMargin(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_ACCOUNT_MARGIN_API + "/latest", RequestType.LATEST, AccountMarginModel.class);
        this.testQueryCompleteUrl(context, QUERY_ACCOUNT_MARGIN_API + "/history", RequestType.HISTORY, AccountMarginModel.class);
    }

    // LiquiGroup Margin

    @Test
    public void testStoreLiquiGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testStoreLiquiGroupMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testStoreLiquiGroupMarginMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testStoreLiquiGroupMarginUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendLiquiGroupMarginData);
    }

    @Test
    public void testQueryLiquiGroupMargin(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_LIQUI_GROUP_MARGIN_API + "/latest", RequestType.LATEST, LiquiGroupMarginModel.class);
        this.testQueryCompleteUrl(context, QUERY_LIQUI_GROUP_MARGIN_API + "/history", RequestType.HISTORY, LiquiGroupMarginModel.class);
    }

    // LiquiGroupSplit Margin

    @Test
    public void testStoreLiquiSplitGroupMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testStoreLiquiGroupSplitMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testStoreLiquiGroupSplitMarginMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testStoreLiquiGroupSplitMarginUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendLiquiGroupSplitMarginData);
    }

    @Test
    public void testQueryLiquiGroupSplitMargin(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_LIQUI_GROUP_SPLIT_MARGIN_API + "/latest", RequestType.LATEST, LiquiGroupSplitMarginModel.class);
        this.testQueryCompleteUrl(context, QUERY_LIQUI_GROUP_SPLIT_MARGIN_API + "/history", RequestType.HISTORY, LiquiGroupSplitMarginModel.class);
    }

    // Pool Margin

    @Test
    public void testStorePoolMargin(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testStorePoolMarginError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testStorePoolMarginMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testStorePoolMarginUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POOL_MARGIN_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendPoolMarginData);
    }

    @Test
    public void testQueryPoolMargin(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_POOL_MARGIN_API + "/latest", RequestType.LATEST, PoolMarginModel.class);
        this.testQueryCompleteUrl(context, QUERY_POOL_MARGIN_API + "/history", RequestType.HISTORY, PoolMarginModel.class);
    }

    // Position Report

    @Test
    public void testStorePositionReport(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testStorePositionReportError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testStorePositionReportMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testStorePositionReportUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.POSITION_REPORT_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendPositionReportData);
    }

    @Test
    public void testQueryPositionReport(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_POSITION_REPORT_API + "/latest", RequestType.LATEST, PositionReportModel.class);
        this.testQueryCompleteUrl(context, QUERY_POSITION_REPORT_API + "/history", RequestType.HISTORY, PositionReportModel.class);
    }

    // Risk Limit Utilization

    @Test
    public void testStoreRiskLimitUtilization(TestContext context) {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStore(context, msgCount, new RestSenderRegular(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testStoreRiskLimitUtilizationError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStoreError(context, msgCount, "Failed to store the document", new RestSenderRegularIgnoreError(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testStoreRiskLimitUtilizationMissingFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStoreError(context, msgCount, "Missing fields in model:", new RestSenderMissingFieldIgnoreError(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testStoreRiskLimitUtilizationUnknownFieldError(TestContext context) throws InterruptedException {
        int msgCount = DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1) + DataHelper.getJsonObjectCount(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 2);
        this.testStoreError(context, msgCount, "Unknown fields in model:", new RestSenderUnknownFieldIgnoreError(this.vertx)::sendRiskLimitUtilizationData);
    }

    @Test
    public void testQueryRiskLimitUtilization(TestContext context) {
        this.testQueryCompleteUrl(context, QUERY_RISK_LIMIT_UTILIZATION_API + "/latest", RequestType.LATEST, RiskLimitUtilizationModel.class);
        this.testQueryCompleteUrl(context, QUERY_RISK_LIMIT_UTILIZATION_API + "/history", RequestType.HISTORY, RiskLimitUtilizationModel.class);
    }

    @Test
    public void testSSLClientAuthentication(TestContext context) {
        EchoPersistenceService persistenceService = new EchoPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        JsonObject httpConfig = TestConfig.getApiConfig();
        httpConfig.put("sslRequireClientAuth", true);
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(httpConfig);
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async asyncWithoutCert = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).get(TestConfig.API_PORT, "localhost", QUERY_POSITION_REPORT_API + "/latest", res -> {
            context.fail("Connected to HTTPS with required client authentication without certificate!");
            }).exceptionHandler(res -> asyncWithoutCert.complete()).end();
        asyncWithoutCert.awaitSuccess(30000);

        final Async asyncWithCert = context.async();
        httpClientOptions.setPemKeyCertOptions(TestConfig.API_CLIENT_CERTIFICATE.keyCertOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", QUERY_POSITION_REPORT_API + "/latest", res -> {
            context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
            asyncWithCert.complete();
        });

        asyncWithCert.awaitSuccess(30000);
        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testQueryIncompleteUrl(TestContext context) {
        JsonObject queryParams = new JsonObject()
                .put("clearer", "CLEARER")
                .put("member", "MEMBER")
                .put("account", "ACCOUNT");

        JsonArray expectedResult = new JsonArray().add(new JsonObject()
                .put("model", "PositionReportModel")
                .put("requestType", "LATEST")
                .mergeIn(queryParams));
        EchoPersistenceService persistenceService = new EchoPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
            res.bodyHandler(body -> {
                JsonArray bd = body.toJsonArray();
                context.assertEquals(expectedResult, bd);
                async.complete();
            });
        });

        async.awaitSuccess(300000);
        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testQueryBadDataType(TestContext context) {
        JsonObject queryParams = new JsonObject()
                .put("clearer", "CLEARER")
                .put("member", "MEMBER")
                .put("contractYear", 1234.5d);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), res.statusCode());
            async.complete();
        });

        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryUnknownParameter(TestContext context) {
        JsonObject queryParams = new JsonObject()
                .put("clearer", "CLEARER")
                .put("member", "MEMBER")
                .put("foo", 2016.2);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), res.statusCode());
            async.complete();
        });

        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryUnknownModel(TestContext context) {
        JsonObject queryParams = new JsonObject()
                .put("clearer", "CLEARER")
                .put("member", "MEMBER");

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(String.format("%s/query/unknown/latest", ApiVerticle.API_PREFIX)).addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), res.statusCode());
            async.complete();
        });

        async.awaitSuccess(30000);
    }

    @Test
    public void testQueryError(TestContext context) throws InterruptedException {
        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        JsonObject queryParams = new JsonObject()
                .put("clearer", "CLEARER")
                .put("member", "MEMBER")
                .put("account", "ACCOUNT");
        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(QUERY_POSITION_REPORT_API + "/latest").addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), res.statusCode());
            async.complete();
        });
        async.awaitSuccess(30000);

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @Test
    public void testStoreUnknownModel(TestContext context) {
        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async async = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).request(HttpMethod.POST,
                TestConfig.API_PORT,
                "localhost",
                String.format("%s/store/unknown", ApiVerticle.API_PREFIX),
                response -> {
                    context.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.statusCode());
                    async.complete();
                })
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(new JsonObject().encode());

        async.awaitSuccess(30000);
    }

    private void testStore(TestContext context, int msgCount, Consumer<Handler<AsyncResult<Void>>> sender) {
        Async async = context.async(msgCount);

        CountdownPersistenceService persistenceService = new CountdownPersistenceService(async);
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

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

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        sender.accept(context.asyncAssertSuccess());
        testAppender.waitForMessageCount(Level.ERROR, msgCount);
        testAppender.waitForMessageContains(Level.ERROR, errorMessage);
        testAppender.stop();
        rootLogger.addAppender(stdout);

        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    private void testQueryCompleteUrl(TestContext context, String uri, RequestType requestType, Class<? extends AbstractModel> modelClazz) {
        EchoPersistenceService persistenceService = new EchoPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        JsonObject queryParams = DataHelper.getQueryParams(DataHelper.getLastModelFromFile(modelClazz, 1));

        JsonArray expectedResult = new JsonArray().add(new JsonObject()
                .put("model", modelClazz.getSimpleName())
                .put("requestType", requestType)
                .mergeIn(queryParams)
        );

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(TestConfig.getApiConfig());
        Async asyncDeploy = context.async();
        vertx.deployVerticle(ApiVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess(ar -> asyncDeploy.complete()));
        asyncDeploy.awaitSuccess();

        final Async asyncQuerySent = context.async();
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions());
        vertx.createHttpClient(httpClientOptions).getNow(TestConfig.API_PORT, "localhost", new URIBuilder(uri).addParams(queryParams).build(), res -> {
            context.assertEquals(HttpResponseStatus.OK.code(), res.statusCode());
            res.bodyHandler(body -> {
                JsonArray bd = body.toJsonArray();
                context.assertEquals(expectedResult, bd);
                asyncQuerySent.complete();
            });
        });
        asyncQuerySent.awaitSuccess(30000);
        ProxyHelper.unregisterService(serviceMessageConsumer);
    }

    @After
    public void cleanup(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
        testAppender.clear();
    }
}