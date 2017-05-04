package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    private static final int RECONNECT_DELAY = 2000;
    static final String ACCOUNT_MARGIN_COLLECTION = MongoPersistenceService.getCollectionName(AccountMarginModel.class);
    static final String LIQUI_GROUP_MARGIN_COLLECTION = MongoPersistenceService.getCollectionName(LiquiGroupMarginModel.class);
    static final String LIQUI_GROUP_SPLIT_MARGIN_COLLECTION = MongoPersistenceService.getCollectionName(LiquiGroupSplitMarginModel.class);
    static final String POOL_MARGIN_COLLECTION = MongoPersistenceService.getCollectionName(PoolMarginModel.class);
    static final String POSITION_REPORT_COLLECTION = MongoPersistenceService.getCollectionName(PositionReportModel.class);
    static final String RISK_LIMIT_UTILIZATION_COLLECTION = MongoPersistenceService.getCollectionName(RiskLimitUtilizationModel.class);

    private final Vertx vertx;
    private final MongoClient mongo;
    private final HealthCheck healthCheck;
    private boolean closed;
    private final ConnectionManager connectionManager = new ConnectionManager();

    @Inject
    public MongoPersistenceService(Vertx vertx, MongoClient mongo) {
        this.vertx = vertx;
        this.healthCheck = new HealthCheck(this.vertx);
        this.mongo = mongo;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        this.createMissingCollections()
                .compose(i -> createIndexes())
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        healthCheck.setComponentReady(Component.PERSISTENCE_SERVICE);
                    } else {
                        if (!closed) {
                            // Try to re-initialize in a few seconds
                            vertx.setTimer(RECONNECT_DELAY, i -> initialize(res -> {/*empty handler*/}));
                        }
                        LOG.error("Initialize failed, trying again...");
                    }
                    // Inform the caller that we succeeded even if the connection to mongo database
                    // failed. We will try to reconnect automatically on background.
                    resultHandler.handle(Future.succeededFuture());
                });
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, ACCOUNT_MARGIN_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, LIQUI_GROUP_MARGIN_COLLECTION, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, resultHandler);
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, POOL_MARGIN_COLLECTION, resultHandler);
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, POSITION_REPORT_COLLECTION, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, RISK_LIMIT_UTILIZATION_COLLECTION, resultHandler);
    }

    @Override
    public void queryAccountMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, ACCOUNT_MARGIN_COLLECTION, query, new AccountMarginModel(), resultHandler);
    }

    @Override
    public void queryLiquiGroupMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, LIQUI_GROUP_MARGIN_COLLECTION, query, new LiquiGroupMarginModel(), resultHandler);
    }

    @Override
    public void queryLiquiGroupSplitMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, query, new LiquiGroupSplitMarginModel(), resultHandler);
    }

    @Override
    public void queryPoolMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, POOL_MARGIN_COLLECTION, query, new PoolMarginModel(), resultHandler);
    }

    @Override
    public void queryPositionReport(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, POSITION_REPORT_COLLECTION, query, new PositionReportModel(), resultHandler);
    }

    @Override
    public void queryRiskLimitUtilization(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        this.query(type, RISK_LIMIT_UTILIZATION_COLLECTION, query, new RiskLimitUtilizationModel(), resultHandler);
    }

    private void query(RequestType type, String collection, JsonObject query, AbstractModel model, Handler<AsyncResult<String>> resultHandler) {
        LOG.trace("Received {} {} query with message {}", type.name(), collection, query);
        BiFunction<JsonObject, AbstractModel, JsonArray> getPipeline;
        switch(type) {
            case LATEST:
                getPipeline = MongoPersistenceService::getLatestPipeline;
                break;
            case HISTORY:
                getPipeline = MongoPersistenceService::getHistoryPipeline;
                break;
            default:
                LOG.error("Unknown request type {}", type);
                resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unknown request type"));
                return;
        }
        mongo.runCommand("aggregate", MongoPersistenceService.getCommand(collection, query, model, getPipeline), res -> {
            if (res.succeeded()) {
                resultHandler.handle(Future.succeededFuture(Json.encodePrettily(res.result().getJsonArray("result"))));
            } else {
                LOG.error("{} query failed", collection, res.cause());
                connectionManager.startReconnection();
                resultHandler.handle(ServiceException.fail(QUERY_ERROR, res.cause().getMessage()));
            }
        });

    }

    private static JsonObject getCommand(String collection, JsonObject params, AbstractModel model, BiFunction<JsonObject, AbstractModel, JsonArray> getPipeline) {
        return new JsonObject()
                .put("aggregate", collection)
                .put("pipeline", getPipeline.apply(params, model))
                .put("allowDiskUse", true);
    }

    private static JsonArray getLatestPipeline(JsonObject params, AbstractModel model) {
        JsonArray pipeline = new JsonArray();
        pipeline.add(new JsonObject().put("$match", params));
        pipeline.add(new JsonObject().put("$project", getLatestSnapshotProject(model)));
        pipeline.add(new JsonObject().put("$unwind", "$snapshots"));
        pipeline.add(new JsonObject().put("$project", getFlattenProject(model)));
        return pipeline;
    }

    private static JsonArray getHistoryPipeline(JsonObject params, AbstractModel model) {
        JsonArray pipeline = new JsonArray();
        pipeline.add(new JsonObject().put("$match", params));
        pipeline.add(new JsonObject().put("$unwind", "$snapshots"));
        pipeline.add(new JsonObject().put("$project", getFlattenProject(model)));
        return pipeline;
    }

    private static JsonObject getLatestSnapshotProject(AbstractModel model) {
        JsonObject project = new JsonObject();
        model.getKeys().forEach(key -> project.put(key, 1));
        project.put("snapshots", new JsonObject().put("$slice", new JsonArray().add("$snapshots").add(-1)));
        return project;
    }

    private static JsonObject getFlattenProject(AbstractModel model) {
        JsonObject project = new JsonObject();
        project.put("_id", 0);
        model.getKeys().forEach(key -> project.put(key, 1));
        model.getNonKeys().forEach(nonKey -> project.put(nonKey, "$snapshots." + nonKey));
        model.getHeader().forEach(header -> project.put(header, "$snapshots." + header));
        return project;
    }

    @Override
    public void close() {
        this.closed = true;
        this.mongo.close();
    }

    private void store(Collection<? extends AbstractModel> models, String collection, Handler<AsyncResult<Void>> resultHandler) {
        this.storeIntoCollection(models, collection).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                connectionManager.startReconnection();
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
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

    private Future<Void> storeIntoCollection(Collection<? extends AbstractModel> models, String collection) {
        List<Future> futureList = new ArrayList<>();
        models.forEach(model -> {
            Future<MongoClientUpdateResult> future = Future.future();
            JsonObject queryParams = MongoPersistenceService.getQueryParams(model);
            JsonObject document = MongoPersistenceService.getStoreDocument(model);
            LOG.trace("Storing message into {} with body {}", collection, document.encodePrettily());
            mongo.updateCollectionWithOptions(collection,
                    queryParams,
                    document,
                    new UpdateOptions().setUpsert(true),
                    future);
            futureList.add(future);
        });
        return CompositeFuture.all(futureList).map((Void) null);
    }

    private Future<Void> createMissingCollections() {
        return getMissingCollections()
                .compose(this::createCollections);
    }

    private Future<List<String>> getMissingCollections() {
        Future<List<String>> missingCollectionsFuture = Future.future();
        mongo.getCollections(res -> {
            if (res.succeeded()) {
                List<String> mongoCollections = res.result();
                List<String> missingCollections = MongoPersistenceService.getRequiredCollections().stream()
                        .filter(collection -> !mongoCollections.contains(collection))
                        .collect(Collectors.toList());
                missingCollectionsFuture.complete(missingCollections);
            } else {
                LOG.error("Failed to get collection list", res.cause());
                missingCollectionsFuture.fail(res.cause());
            }
        });
        return missingCollectionsFuture;
    }

    private Future<Void> createCollections(List<String> collections) {
        Future<Void> createCollectionsFuture = Future.future();

        List<Future> futs = new ArrayList<>();
        collections.forEach(collection -> {
            LOG.info("Collection {} is missing and will be added", collection);
            Future<Void> fut = Future.future();
            mongo.createCollection(collection, fut);
            futs.add(fut);
        });
        CompositeFuture.all(futs).setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("Mongo has all needed collections for DAVe");
                LOG.info("Initialized MongoDB");
                createCollectionsFuture.complete();
            } else {
                LOG.error("Failed to add all collections needed for DAVe to Mongo", ar.cause());
                createCollectionsFuture.fail(ar.cause());
            }
        });
        return createCollectionsFuture;
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

    private class ConnectionManager {

        void startReconnection() {
            if (healthCheck.isComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE)) {
                // Inform other components that we have failed
                healthCheck.setComponentFailed(HealthCheck.Component.PERSISTENCE_SERVICE);
                // Re-check the connection
                scheduleConnectionStatus();
            }
        }

        private void scheduleConnectionStatus() {
            if (!closed) {
                vertx.setTimer(RECONNECT_DELAY, id -> checkConnectionStatus());
            }
        }

        private void checkConnectionStatus() {
            mongo.runCommand("ping", new JsonObject().put("ping", 1), res -> {
                if (res.succeeded()) {
                    LOG.info("Back online");
                    healthCheck.setComponentReady(HealthCheck.Component.PERSISTENCE_SERVICE);
                } else {
                    LOG.error("Still disconnected");
                    scheduleConnectionStatus();
                }
            });
        }
    }
}
