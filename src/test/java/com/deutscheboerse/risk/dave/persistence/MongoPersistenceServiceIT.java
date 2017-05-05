 package com.deutscheboerse.risk.dave.persistence;

 import ch.qos.logback.classic.Level;
 import ch.qos.logback.classic.Logger;
 import ch.qos.logback.classic.spi.ILoggingEvent;
 import ch.qos.logback.core.Appender;
 import com.deutscheboerse.risk.dave.log.TestAppender;
 import com.deutscheboerse.risk.dave.model.*;
 import com.deutscheboerse.risk.dave.utils.DataHelper;
 import com.deutscheboerse.risk.dave.utils.TestConfig;
 import io.vertx.core.AsyncResult;
 import io.vertx.core.Handler;
 import io.vertx.core.Vertx;
 import io.vertx.core.json.JsonArray;
 import io.vertx.core.json.JsonObject;
 import io.vertx.ext.mongo.impl.MongoBulkClient;
 import io.vertx.ext.unit.Async;
 import io.vertx.ext.unit.TestContext;
 import io.vertx.ext.unit.junit.VertxUnitRunner;
 import io.vertx.serviceproxy.ProxyHelper;
 import org.junit.*;
 import org.junit.runner.RunWith;
 import org.slf4j.LoggerFactory;

 import java.io.IOException;
 import java.util.*;
 import java.util.function.BiConsumer;
 import java.util.function.Function;
 import java.util.stream.Collectors;
 import java.util.stream.IntStream;

 @RunWith(VertxUnitRunner.class)
public class MongoPersistenceServiceIT {
    private static final TestAppender testAppender = TestAppender.getAppender(MongoPersistenceService.class);
    private static final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static Vertx vertx;
    private static MongoBulkClient mongoClient;
    private static PersistenceService persistenceProxy;

    @BeforeClass
    public static void setUp(TestContext context) {
        MongoPersistenceServiceIT.vertx = Vertx.vertx();
        JsonObject config = TestConfig.getMongoConfig();
        JsonObject mongoConfig = TestConfig.getMongoClientConfig(config);

        MongoPersistenceServiceIT.mongoClient = MongoBulkClient.createShared(MongoPersistenceServiceIT.vertx, mongoConfig);

        ProxyHelper.registerService(PersistenceService.class, vertx, new MongoPersistenceService(vertx, mongoClient), PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
        MongoPersistenceServiceIT.persistenceProxy.initialize(context.asyncAssertSuccess());

        rootLogger.addAppender(testAppender);
    }

    @Test
    public void checkCollectionsExist(TestContext context) {
        List<String> requiredCollections = new ArrayList<>();
        requiredCollections.addAll(MongoPersistenceService.getRequiredCollections());
        final Async async = context.async();
        MongoPersistenceServiceIT.mongoClient.getCollections(ar -> {
            if (ar.succeeded()) {
                if (ar.result().containsAll(requiredCollections)) {
                    async.complete();
                } else {
                    requiredCollections.removeAll(ar.result());
                    context.fail("Following collections were not created: " + requiredCollections);
                }
            } else {
                context.fail(ar.cause());
            }
        });
    }

    @Test
    public void checkIndexesExist(TestContext context) {
        // one index for history and one for latest collection in each model
        final Async async = context.async(MongoPersistenceService.getRequiredCollections().size());
        BiConsumer<String, JsonObject> indexCheck = (collectionName, expectedIndex) ->
                MongoPersistenceServiceIT.mongoClient.listIndexes(collectionName, ar -> {
                if (ar.succeeded()) {
                    JsonArray result = ar.result();
                    Optional<Object> latestUniqueIndex = result.stream()
                            .filter(index -> index instanceof JsonObject)
                            .filter(index -> ((JsonObject) index).getString("name", "").equals("unique_idx"))
                            .filter(index -> ((JsonObject) index).getJsonObject("key", new JsonObject()).equals(expectedIndex))
                            .findFirst();
                    if (latestUniqueIndex.isPresent()) {
                        async.countDown();
                    } else {
                        context.fail("Missing unique index for collection " + collectionName);
                    }
                } else {
                    context.fail("Unable to list indexes from collection " + collectionName);
                }
            });

        indexCheck.accept(MongoPersistenceService.ACCOUNT_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new AccountMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new LiquiGroupMarginModel()));
        indexCheck.accept(MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new LiquiGroupSplitMarginModel()));
        indexCheck.accept(MongoPersistenceService.POOL_MARGIN_COLLECTION, MongoPersistenceService.getUniqueIndex(new PoolMarginModel()));
        indexCheck.accept(MongoPersistenceService.POSITION_REPORT_COLLECTION, MongoPersistenceService.getUniqueIndex(new PositionReportModel()));
        indexCheck.accept(MongoPersistenceService.RISK_LIMIT_UTILIZATION_COLLECTION, MongoPersistenceService.getUniqueIndex(new RiskLimitUtilizationModel()));
    }

    @Test
    public void testGetCollectionsError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "getCollections", "Failed to get collection list");
    }

    @Test
    public void testCreateCollectionError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "createCollection", "Failed to add all collections");
    }

    @Test
    public void testCreateIndexWithOptionsError(TestContext context) throws InterruptedException {
        this.testErrorInInitialize(context, "createIndexWithOptions", "Failed to create all needed indexes in Mongo");
    }

    @Test
    public void testConnectionStatusBackOnline(TestContext context) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("bulkWrite"));

        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        final AccountMarginModel model = new AccountMarginModel(new JsonObject().put("timestamp", 0L));

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        persistenceErrorProxy.storeAccountMargin(Collections.singletonList(model), context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.INFO, "Back online");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        persistenceErrorProxy.close();
    }

    @Test
    public void testConnectionStatusErrorAfterStore(TestContext context) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("bulkWrite").add("runCommand"));

        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        final AccountMarginModel model = new AccountMarginModel(new JsonObject().put("timestamp", 0L));

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        persistenceErrorProxy.storeAccountMargin(Collections.singletonList(model), context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.ERROR, "Still disconnected");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        persistenceErrorProxy.close();
    }

    @Test
    public void testConnectionStatusErrorAfterQuery(TestContext context) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add("runCommand"));

        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());

        Appender<ILoggingEvent> stdout = rootLogger.getAppender("STDOUT");
        rootLogger.detachAppender(stdout);
        testAppender.start();
        AccountMarginModel model = DataHelper.getLastModelFromFile(DataHelper.ACCOUNT_MARGIN_FOLDER, 1, AccountMarginModel::new);
        persistenceErrorProxy.queryAccountMargin(RequestType.LATEST, DataHelper.getQueryParams(model), context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.ERROR, "Still disconnected");
        testAppender.stop();
        rootLogger.addAppender(stdout);

        persistenceErrorProxy.close();
    }

    @Test
    public void testAccountMarginStore(TestContext context) {
        this.testStore(context,
                DataHelper.ACCOUNT_MARGIN_FOLDER,
                MongoPersistenceService.ACCOUNT_MARGIN_COLLECTION,
                persistenceProxy::storeAccountMargin,
                persistenceProxy::queryAccountMargin,
                AccountMarginModel::new);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        this.testStore(context,
                DataHelper.LIQUI_GROUP_MARGIN_FOLDER,
                MongoPersistenceService.LIQUI_GROUP_MARGIN_COLLECTION,
                persistenceProxy::storeLiquiGroupMargin,
                persistenceProxy::queryLiquiGroupMargin,
                LiquiGroupMarginModel::new);
    }

    @Test
    public void testLiquiGroupSplitMarginStore(TestContext context) throws IOException {
        this.testStore(context,
                DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER,
                MongoPersistenceService.LIQUI_GROUP_SPLIT_MARGIN_COLLECTION,
                persistenceProxy::storeLiquiGroupSplitMargin,
                persistenceProxy::queryLiquiGroupSplitMargin,
                LiquiGroupSplitMarginModel::new);
    }

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        this.testStore(context,
                DataHelper.POOL_MARGIN_FOLDER,
                MongoPersistenceService.POOL_MARGIN_COLLECTION,
                persistenceProxy::storePoolMargin,
                persistenceProxy::queryPoolMargin,
                PoolMarginModel::new);
    }

    @Test
    public void testPositionReportStore(TestContext context) throws IOException {
        this.testStore(context,
                DataHelper.POSITION_REPORT_FOLDER,
                MongoPersistenceService.POSITION_REPORT_COLLECTION,
                persistenceProxy::storePositionReport,
                persistenceProxy::queryPositionReport,
                PositionReportModel::new);
    }

    @Test
    public void testRiskLimitUtilizationStore(TestContext context) throws IOException {
        this.testStore(context,
                DataHelper.RISK_LIMIT_UTILIZATION_FOLDER,
                MongoPersistenceService.RISK_LIMIT_UTILIZATION_COLLECTION,
                persistenceProxy::storeRiskLimitUtilization,
                persistenceProxy::queryRiskLimitUtilization,
                RiskLimitUtilizationModel::new);
    }

    private interface StoreFunction<T extends AbstractModel> {
        void store(List<T> models, Handler<AsyncResult<Void>> resultHandler);
    }

    private interface QueryFunction {
        void query(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    }

    private <T extends AbstractModel>
    void testStore(TestContext context, String dataFolder, String collection,
                   StoreFunction<T> storeFunction, QueryFunction queryFunction,
                   Function<JsonObject, T> modelFactory) {

        IntStream.rangeClosed(1, 2).forEach(ttsaveNo -> {
            Async asyncStore = context.async(1);
            List<T> snapshot = DataHelper.readTTSaveFile(dataFolder, ttsaveNo)
                    .stream()
                    .map(modelFactory)
                    .collect(Collectors.toList());
            storeFunction.store(snapshot, ar -> {
                if (ar.succeeded()) {
                    asyncStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
            asyncStore.awaitSuccess(30000);
        });

        int expectedCollectionCount = DataHelper.getJsonObjectCount(dataFolder, 1);
        this.checkCountInCollection(context, collection, expectedCollectionCount);

        T firstModel = DataHelper.getLastModelFromFile(dataFolder, 1, modelFactory);
        T secondModel = DataHelper.getLastModelFromFile(dataFolder, 2, modelFactory);

        checkMongoCollection(context, firstModel, secondModel, collection);

        queryFunction.query(RequestType.HISTORY, DataHelper.getQueryParams(firstModel), context.asyncAssertSuccess(res ->
                context.assertEquals(firstModel.toJson(), new JsonArray(res).getJsonObject(0))
        ));
        queryFunction.query(RequestType.LATEST, DataHelper.getQueryParams(secondModel), context.asyncAssertSuccess(res ->
                context.assertEquals(secondModel.toJson(), new JsonArray(res).getJsonObject(0))
        ));
    }

    private void testErrorInInitialize(TestContext context, String functionToFail, String expectedErrorMessage) throws InterruptedException {
        JsonObject proxyConfig = new JsonObject().put("functionsToFail", new JsonArray().add(functionToFail));
        final PersistenceService persistenceErrorProxy = getPersistenceErrorProxy(proxyConfig);

        testAppender.start();
        persistenceErrorProxy.initialize(context.asyncAssertSuccess());
        testAppender.waitForMessageContains(Level.ERROR, expectedErrorMessage);
        testAppender.waitForMessageContains(Level.ERROR, "Initialize failed, trying again...");
        testAppender.stop();

        persistenceErrorProxy.close();
    }

    private void checkCountInCollection(TestContext context, String collection, long count) {
        Async asyncHistoryCount = context.async();
        MongoPersistenceServiceIT.mongoClient.count(collection, new JsonObject(), ar -> {
            if (ar.succeeded()) {
                context.assertEquals(count, ar.result());
                asyncHistoryCount.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncHistoryCount.awaitSuccess(5000);
    }

    private void checkMongoCollection(TestContext context, AbstractModel firstSnapshotModel, AbstractModel secondSnapshotModel, String collectionName) {
        JsonObject param = MongoPersistenceServiceIT.getQueryParams(firstSnapshotModel);
        Assert.assertEquals(param, MongoPersistenceServiceIT.getQueryParams(secondSnapshotModel));
        Async asyncQuery = context.async();
        mongoClient.find(collectionName, param, ar -> {
            if (ar.succeeded()) {
                context.assertEquals(1, ar.result().size());
                JsonObject result = ar.result().get(0);
                JsonArray snapshots = result.getJsonArray("snapshots");
                Assert.assertEquals(2, snapshots.size());
                MongoPersistenceServiceIT.assertSnapshotsContains(context, snapshots, firstSnapshotModel, 0);
                MongoPersistenceServiceIT.assertSnapshotsContains(context, snapshots, secondSnapshotModel, 1);
                asyncQuery.complete();
            } else {
                context.fail(ar.cause());
            }
        });
        asyncQuery.awaitSuccess(5000);
    }

    private PersistenceService getPersistenceErrorProxy(JsonObject config) {
        MongoErrorClient mongoErrorClient = new MongoErrorClient(config);

        final String serviceAddress = PersistenceService.SERVICE_ADDRESS + UUID.randomUUID().getLeastSignificantBits();
        ProxyHelper.registerService(PersistenceService.class, vertx, new MongoPersistenceService(vertx, mongoErrorClient), serviceAddress);
        return ProxyHelper.createProxy(PersistenceService.class, vertx, serviceAddress);
    }

    private static JsonObject getQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    private static void assertSnapshotsContains(TestContext context, JsonArray snapshots, AbstractModel model, int position) {
        JsonObject snapshotData = new JsonObject();
        model.stream()
                .filter(entry -> !model.getKeys().contains(entry.getKey()))
                .forEach(entry -> snapshotData.put(entry.getKey(), entry.getValue()));
        context.assertEquals(snapshotData, snapshots.getJsonObject(position));
    }

    @After
    public void cleanup() {
        MongoPersistenceServiceIT.testAppender.clear();
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        MongoPersistenceServiceIT.rootLogger.detachAppender(testAppender);
        MongoPersistenceServiceIT.persistenceProxy.close();
        MongoPersistenceServiceIT.vertx.close(context.asyncAssertSuccess());
    }
}
