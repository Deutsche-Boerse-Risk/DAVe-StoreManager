package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import com.deutscheboerse.risk.dave.model.*;
import com.google.common.collect.Lists;
import com.mongodb.MongoBulkWriteException;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.ext.mongo.MongoClientBulkWriteResult;
import io.vertx.ext.mongo.impl.MongoBulkClient;
import io.vertx.serviceproxy.ServiceException;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

public class MongoPersistenceService implements PersistenceService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoPersistenceService.class);

    private static final int RECONNECT_DELAY = 2000;
    private static final int MONGO_DUPLICATE_KEY_ERROR_CODE = 11000;
    private static final int RETRIES_IF_UPSERT_FAILED = 3;

    static final String ACCOUNT_MARGIN_COLLECTION = AccountMarginModel.getMongoModelDescriptor().getCollectionName();
    static final String LIQUI_GROUP_MARGIN_COLLECTION = LiquiGroupMarginModel.getMongoModelDescriptor().getCollectionName();
    static final String LIQUI_GROUP_SPLIT_MARGIN_COLLECTION = LiquiGroupSplitMarginModel.getMongoModelDescriptor().getCollectionName();
    static final String POOL_MARGIN_COLLECTION = PoolMarginModel.getMongoModelDescriptor().getCollectionName();
    static final String POSITION_REPORT_COLLECTION = PositionReportModel.getMongoModelDescriptor().getCollectionName();
    static final String RISK_LIMIT_UTILIZATION_COLLECTION = RiskLimitUtilizationModel.getMongoModelDescriptor().getCollectionName();
    private static final int MONGO_BULK_WRITE_SIZE = 100;

    private final Vertx vertx;
    private final MongoBulkClient mongo;
    private final HealthCheck healthCheck;
    private boolean closed;
    private final ConnectionManager connectionManager = new ConnectionManager();

    @Inject
    public MongoPersistenceService(Vertx vertx, MongoBulkClient mongo) {
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
    public void queryAccountMargin(RequestType type, JsonObject query, Handler<AsyncResult<List<AccountMarginModel>>> resultHandler) {
        this.query(type, ACCOUNT_MARGIN_COLLECTION, query, AccountMarginModel.getMongoModelDescriptor(), AccountMarginModel.class, resultHandler);
    }

    @Override
    public void queryLiquiGroupMargin(RequestType type, JsonObject query, Handler<AsyncResult<List<LiquiGroupMarginModel>>> resultHandler) {
        this.query(type, LIQUI_GROUP_MARGIN_COLLECTION, query, LiquiGroupMarginModel.getMongoModelDescriptor(), LiquiGroupMarginModel.class, resultHandler);
    }

    @Override
    public void queryLiquiGroupSplitMargin(RequestType type, JsonObject query, Handler<AsyncResult<List<LiquiGroupSplitMarginModel>>> resultHandler) {
        this.query(type, LIQUI_GROUP_SPLIT_MARGIN_COLLECTION, query, LiquiGroupSplitMarginModel.getMongoModelDescriptor(), LiquiGroupSplitMarginModel.class, resultHandler);
    }

    @Override
    public void queryPoolMargin(RequestType type, JsonObject query, Handler<AsyncResult<List<PoolMarginModel>>> resultHandler) {
        this.query(type, POOL_MARGIN_COLLECTION, query, PoolMarginModel.getMongoModelDescriptor(), PoolMarginModel.class, resultHandler);
    }

    @Override
    public void queryPositionReport(RequestType type, JsonObject query, Handler<AsyncResult<List<PositionReportModel>>> resultHandler) {
        this.query(type, POSITION_REPORT_COLLECTION, query, PositionReportModel.getMongoModelDescriptor(), PositionReportModel.class, resultHandler);
    }

    @Override
    public void queryRiskLimitUtilization(RequestType type, JsonObject query, Handler<AsyncResult<List<RiskLimitUtilizationModel>>> resultHandler) {
        this.query(type, RISK_LIMIT_UTILIZATION_COLLECTION, query, RiskLimitUtilizationModel.getMongoModelDescriptor(), RiskLimitUtilizationModel.class, resultHandler);
    }

    private <T extends Model>
    void query(RequestType type, String collection, JsonObject query, MongoModelDescriptor modelDescriptor, Class<T> modelType, Handler<AsyncResult<List<T>>> resultHandler) {
        LOG.trace("Received {} {} query with message {}", type.name(), collection, query);
        JsonArray pipeline = getPipeline(type, query, modelDescriptor);

        mongo.aggregate(collection, pipeline, modelType, res -> {
            if (res.succeeded()) {
                resultHandler.handle(Future.succeededFuture(res.result()));
            } else {
                LOG.error("{} query failed", collection, res.cause());
                connectionManager.startReconnection();
                resultHandler.handle(ServiceException.fail(QUERY_ERROR, res.cause().getMessage()));
            }
        });
    }

    private JsonArray getPipeline(RequestType type, JsonObject query, MongoModelDescriptor modelDescriptor) {
        switch(type) {
            case LATEST:
                return MongoPersistenceService.getLatestPipeline(query, modelDescriptor);
            case HISTORY:
                return MongoPersistenceService.getHistoryPipeline(query, modelDescriptor);
            default:
                throw new AssertionError();
        }
    }

    private static JsonArray getLatestPipeline(JsonObject params, MongoModelDescriptor modelDescriptor) {
        JsonArray pipeline = new JsonArray();
        pipeline.add(new JsonObject().put("$match", params));
        pipeline.add(new JsonObject().put("$project", getLatestSnapshotProject(modelDescriptor)));
        pipeline.add(new JsonObject().put("$unwind", "$snapshots"));
        pipeline.add(new JsonObject().put("$project", getFlattenProject(modelDescriptor)));
        return pipeline;
    }

    private static JsonArray getHistoryPipeline(JsonObject params, MongoModelDescriptor modelDescriptor) {
        JsonArray pipeline = new JsonArray();
        pipeline.add(new JsonObject().put("$match", params));
        pipeline.add(new JsonObject().put("$unwind", "$snapshots"));
        pipeline.add(new JsonObject().put("$project", getFlattenProject(modelDescriptor)));
        return pipeline;
    }

    private static JsonObject getLatestSnapshotProject(MongoModelDescriptor modelDescriptor) {
        JsonObject project = new JsonObject();
        modelDescriptor.getCommonFields().forEach(field -> project.put(field, 1));
        project.put("snapshots", new JsonObject().put("$slice", new JsonArray().add("$snapshots").add(-1)));
        return project;
    }

    private static JsonObject getFlattenProject(MongoModelDescriptor modelDescriptor) {
        JsonObject project = new JsonObject();
        project.put("_id", 0);
        modelDescriptor.getCommonFields().forEach(field -> project.put(field, 1));
        modelDescriptor.getSnapshotFields().forEach(field -> project.put(field, "$snapshots." + field));
        return project;
    }

    @Override
    public void close() {
        this.closed = true;
        this.mongo.close();
    }

    private void store(List<? extends Model> models, String collection, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models, collection, RETRIES_IF_UPSERT_FAILED, resultHandler);
    }

    private void store(List<? extends Model> models, String collection, int remainingRetries, Handler<AsyncResult<Void>> resultHandler) {
        this.storeIntoCollection(models, collection).setHandler(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture());
            } else if (ar.cause() instanceof MongoBulkWriteException
                    && ((MongoBulkWriteException)ar.cause()).getWriteErrors().stream()
                        .anyMatch(bulkWriteError -> bulkWriteError.getCode() == MONGO_DUPLICATE_KEY_ERROR_CODE)
                    && remainingRetries > 1) {
                LOG.warn("Upsert failed - known Mongo issue, retrying ... ", ar.cause());
                store(models, collection, remainingRetries - 1, resultHandler);
            } else {
                connectionManager.startReconnection();
                resultHandler.handle(ServiceException.fail(STORE_ERROR, ar.cause().getMessage()));
            }
        });
    }

    static Collection<String> getRequiredCollections() {
        List<String> neededCollections = new ArrayList<>(Arrays.asList(
                ACCOUNT_MARGIN_COLLECTION,
                LIQUI_GROUP_MARGIN_COLLECTION,
                LIQUI_GROUP_SPLIT_MARGIN_COLLECTION,
                POOL_MARGIN_COLLECTION,
                POSITION_REPORT_COLLECTION,
                RISK_LIMIT_UTILIZATION_COLLECTION
        ));
        return Collections.unmodifiableList(neededCollections);
    }

    private Future<Void> storeIntoCollection(List<? extends Model> models, String collection) {
        List<Future> futureList = new ArrayList<>();

        Lists.partition(models, MONGO_BULK_WRITE_SIZE).forEach(modelsBulk -> {
            List<BulkOperation> bulkWrites = modelsBulk.stream()
                    .peek(model -> {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Storing message into {} with body {}", collection,
                                    model.getMongoStoreDocument().encodePrettily());
                        }
                    })
                    .map(model -> BulkOperation.createUpdate(model.getMongoQueryParams(), model.getMongoStoreDocument())
                            .setUpsert(true))
                    .collect(Collectors.toList());
            Future<MongoClientBulkWriteResult> bulkResult = Future.future();
            this.mongo.bulkWrite(collection, bulkWrites, bulkResult);
            futureList.add(bulkResult);
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
                AccountMarginModel.getMongoModelDescriptor(),
                LiquiGroupMarginModel.getMongoModelDescriptor(),
                LiquiGroupSplitMarginModel.getMongoModelDescriptor(),
                PoolMarginModel.getMongoModelDescriptor(),
                PositionReportModel.getMongoModelDescriptor(),
                RiskLimitUtilizationModel.getMongoModelDescriptor()
        ).forEach(model -> {
            IndexOptions indexOptions = new IndexOptions().name("unique_idx").unique(true);
            Future<Void> indexFuture = Future.future();
            mongo.createIndexWithOptions(model.getCollectionName(), model.getIndex(), indexOptions, indexFuture);
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
