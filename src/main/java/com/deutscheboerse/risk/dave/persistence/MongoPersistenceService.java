package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.MongoClientUpdateResult;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.serviceproxy.ServiceException;

import javax.inject.Inject;
import java.util.*;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    private static final int RECONNECT_DELAY = 2000;

    private final Vertx vertx;
    private final MongoClient mongo;
    private final HealthCheck healthCheck;

    @Inject
    public MongoPersistenceService(Vertx vertx, MongoClient mongo) {
        this.vertx = vertx;
        this.healthCheck = new HealthCheck(this.vertx);
        this.mongo = mongo;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        initDb()
                .compose(i -> createIndexes())
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        healthCheck.setComponentReady(Component.PERSISTENCE_SERVICE);
                    } else {
                        // Try to re-initialize in a few seconds
                        vertx.setTimer(RECONNECT_DELAY, i -> initialize(res -> {/*empty handler*/}));
                        LOG.error("Initialize failed, trying again...");
                    }
                    // Inform the caller that we succeeded even if the connection to mongo database
                    // failed. We will try to reconnect automatically on background.
                    resultHandler.handle(Future.succeededFuture());
                });
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(AccountMarginModel.class), resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(LiquiGroupMarginModel.class), resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(LiquiGroupSplitMarginModel.class), resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(PoolMarginModel.class), resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(PositionReportModel.class), resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, MongoPersistenceService.getCollectionName(RiskLimitUtilizationModel.class), resultHandler);
    }

    @Override
    public void close() {
        this.mongo.close();
    }

    private void store(AbstractModel model, String collection, Handler<AsyncResult<Void>> resultHandler) {
        this.storeIntoCollection(model, collection).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                if (healthCheck.isComponentReady(Component.PERSISTENCE_SERVICE)) {
                    // Inform other components that we have failed
                    healthCheck.setComponentFailed(Component.PERSISTENCE_SERVICE);
                    // Re-check the connection
                    scheduleConnectionStatus();
                }
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
            }
        });
    }

    private void scheduleConnectionStatus() {
        vertx.setTimer(RECONNECT_DELAY, id -> checkConnectionStatus());
    }

    private void checkConnectionStatus() {
        this.mongo.runCommand("dbstats", new JsonObject().put("dbstats", 1), res -> {
            if (res.succeeded()) {
                LOG.info("Back online");
                healthCheck.setComponentReady(Component.PERSISTENCE_SERVICE);
            } else {
                LOG.error("Still disconnected");
                scheduleConnectionStatus();
            }
        });
    }

    private static JsonObject getQueryParams(AbstractModel model) {
        JsonObject queryParams = new JsonObject();
        model.getKeys().forEach(key -> queryParams.put(key, model.getValue(key)));
        return queryParams;
    }

    static JsonObject getUniqueIndex(AbstractModel model) {
        JsonObject uniqueIndex = new JsonObject();
        model.getKeys().forEach(key -> uniqueIndex.put(key, 1));
        return uniqueIndex;
    }

    private static JsonObject getStoreDocument(AbstractModel model) {
        JsonObject document = new JsonObject();
        JsonObject setDocument = new JsonObject();
        JsonObject pushDocument = new JsonObject();
        model.getKeys().forEach(key -> setDocument.put(key, model.getValue(key)));
        model.stream()
                .filter(entry -> !model.getKeys().contains(entry.getKey()))
                .forEach(entry -> pushDocument.put(entry.getKey(), entry.getValue()));
        document.put("$set", setDocument);
        document.put("$push", new JsonObject().put("snapshots", pushDocument));
        return document;
    }

    public static String getCollectionName(Class<? extends AbstractModel> clazz) {
        return clazz.getSimpleName().replace("Model", "");
    }

    static Collection<String> getRequiredCollections() {
        List<String> neededCollections = new ArrayList<>(Arrays.asList(
                MongoPersistenceService.getCollectionName(AccountMarginModel.class),
                MongoPersistenceService.getCollectionName(LiquiGroupMarginModel.class),
                MongoPersistenceService.getCollectionName(LiquiGroupSplitMarginModel.class),
                MongoPersistenceService.getCollectionName(PoolMarginModel.class),
                MongoPersistenceService.getCollectionName(PositionReportModel.class),
                MongoPersistenceService.getCollectionName(RiskLimitUtilizationModel.class)
        ));
        return Collections.unmodifiableList(neededCollections);
    }

    private Future<MongoClientUpdateResult> storeIntoCollection(AbstractModel model, String collection) {
        JsonObject queryParams = MongoPersistenceService.getQueryParams(model);
        JsonObject document = MongoPersistenceService.getStoreDocument(model);
        LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
        Future<MongoClientUpdateResult> result = Future.future();
        mongo.updateCollectionWithOptions(collection,
                queryParams,
                document,
                new UpdateOptions().setUpsert(true),
                result);
        return result;
    }

    private Future<Void> initDb() {
        Future<Void> initDbFuture = Future.future();
        mongo.getCollections(res -> {
            if (res.succeeded()) {
                List<String> mongoCollections = res.result();
                List<Future> futs = new ArrayList<>();
                MongoPersistenceService.getRequiredCollections().stream()
                        .filter(collection -> ! mongoCollections.contains(collection))
                        .forEach(collection -> {
                            LOG.info("Collection {} is missing and will be added", collection);
                            Future<Void> fut = Future.future();
                            mongo.createCollection(collection, fut);
                            futs.add(fut);
                        });
                CompositeFuture.all(futs).setHandler(ar -> {
                    if (ar.succeeded()) {
                        LOG.info("Mongo has all needed collections for DAVe");
                        LOG.info("Initialized MongoDB");
                        initDbFuture.complete();
                    } else {
                        LOG.error("Failed to add all collections needed for DAVe to Mongo", ar.cause());
                        initDbFuture.fail(ar.cause());
                    }
                });
            } else {
                LOG.error("Failed to get collection list", res.cause());
                initDbFuture.fail(res.cause());
            }
        });
        return initDbFuture;
    }

    private Future<Void> createIndexes() {
        Future<Void> createIndexesFuture = Future.future();

        List<Future> futs = new ArrayList<>();
        Arrays.asList(
                new AccountMarginModel(),
                new LiquiGroupMarginModel(),
                new LiquiGroupSplitMarginModel(),
                new PoolMarginModel(),
                new PositionReportModel(),
                new RiskLimitUtilizationModel()
        ).forEach(model -> {
            String collectionName = MongoPersistenceService.getCollectionName(model.getClass());
            JsonObject index = MongoPersistenceService.getUniqueIndex(model);
            IndexOptions indexOptions = new IndexOptions().name("unique_idx").unique(true);
            Future<Void> indexFuture = Future.future();
            mongo.createIndexWithOptions(collectionName, index, indexOptions, indexFuture);
            futs.add(indexFuture);
        });

        CompositeFuture.all(futs).setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("Mongo has all needed indexes");
                createIndexesFuture.complete();
            } else {
                LOG.error("Failed to create all needed indexes in Mongo", ar.cause());
                createIndexesFuture.fail(ar.cause());
            }
        });
        return createIndexesFuture;
    }
}
