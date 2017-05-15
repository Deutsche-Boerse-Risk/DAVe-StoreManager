package com.deutscheboerse.risk.dave;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.*;
import com.deutscheboerse.risk.dave.restapi.StoreApi;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.GrpcSenderRegular;
import com.deutscheboerse.risk.dave.utils.GrpcSenderRegularIgnoreError;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import com.google.protobuf.MessageLite;
import io.grpc.ManagedChannel;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.TCPSSLOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.grpc.GrpcReadStream;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class ApiVerticleTest {
    private final TestAppender testAppender = TestAppender.getAppender(StoreApi.class);
    private final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private Vertx vertx;
    private ManagedChannel grpcChannel;

    @Before
    public void setUp() throws IOException {
        this.vertx = Vertx.vertx();
        rootLogger.addAppender(testAppender);
        this.grpcChannel = this.createGrpcChannel();
    }

    private ManagedChannel createGrpcChannel() {
        return VertxChannelBuilder
                .forAddress(vertx, "localhost", TestConfig.API_PORT)
                .useSsl(this::setGrpcSslOptions)
                .build();
    }

    private void setGrpcSslOptions(TCPSSLOptions sslOptions) {
        sslOptions
                .setSsl(true)
                .setUseAlpn(true)
                .setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions())
                .setPemKeyCertOptions(TestConfig.API_CLIENT_CERTIFICATE.keyCertOptions());
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
        AccountMarginModel model = DataHelper.getLastModelFromFile(DataHelper.ACCOUNT_MARGIN_FOLDER, 1, AccountMarginModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryAccountMargin);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryAccountMargin);
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
        LiquiGroupMarginModel model = DataHelper.getLastModelFromFile(DataHelper.LIQUI_GROUP_MARGIN_FOLDER, 1, LiquiGroupMarginModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryLiquiGroupMargin);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryLiquiGroupMargin);
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
        LiquiGroupSplitMarginModel model = DataHelper.getLastModelFromFile(DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, 1, LiquiGroupSplitMarginModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryLiquiGroupSplitMargin);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryLiquiGroupSplitMargin);
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
        PoolMarginModel model = DataHelper.getLastModelFromFile(DataHelper.POOL_MARGIN_FOLDER, 1, PoolMarginModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryPoolMargin);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryPoolMargin);
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
        PositionReportModel model = DataHelper.getLastModelFromFile(DataHelper.POSITION_REPORT_FOLDER, 1, PositionReportModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryPositionReport);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryPositionReport);
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
        RiskLimitUtilizationModel model = DataHelper.getLastModelFromFile(DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1, RiskLimitUtilizationModel::buildFromJson);
        this.testQuery(context, RequestType.LATEST, DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryRiskLimitUtilization);
        this.testQuery(context, RequestType.HISTORY, DataHelper.getGrpcQueryFromModel(RequestType.HISTORY, model), PersistenceServiceGrpc.newVertxStub(this.grpcChannel)::queryRiskLimitUtilization);
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
    public void testQueryError(TestContext context) throws InterruptedException {
        ErrorPersistenceService persistenceService = new ErrorPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        this.deployApiVerticle(context);

        final Async async = context.async();
        AccountMarginModel model = DataHelper.getLastModelFromFile(DataHelper.ACCOUNT_MARGIN_FOLDER, 1, AccountMarginModel::buildFromJson);
        PersistenceServiceGrpc.newVertxStub(this.grpcChannel).queryAccountMargin(DataHelper.getGrpcQueryFromModel(RequestType.LATEST, model), request -> {
            request.handler(payload -> context.fail("Error service should not return anything"))
                    .endHandler(v -> context.fail("Error Persistence Service should cause an exception!"))
                    .exceptionHandler(t -> async.complete());
        });
        ProxyHelper.unregisterService(serviceMessageConsumer);
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

    private <Q extends MessageLite, T extends MessageLite>
    void testQuery(TestContext context, RequestType requestType, Q query,
                   BiConsumer<Q, Handler<GrpcReadStream<T>>> queryFunction) {
        EchoPersistenceService persistenceService = new EchoPersistenceService();
        MessageConsumer<JsonObject> serviceMessageConsumer = ProxyHelper.registerService(PersistenceService.class, this.vertx, persistenceService, PersistenceService.SERVICE_ADDRESS);

        this.deployApiVerticle(context);
        final Async async = context.async();

        queryFunction.accept(query, request -> {
            request.handler(payload -> context.fail("Echo service should not return anything"))
                    .endHandler(v -> async.complete());
        });

        async.awaitSuccess(30000);
        ProxyHelper.unregisterService(serviceMessageConsumer);
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
        this.grpcChannel.shutdown();
        this.vertx.close(context.asyncAssertSuccess());
        rootLogger.detachAppender(testAppender);
        testAppender.clear();
    }
}
