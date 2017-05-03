package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.serviceproxy.ServiceException;

import java.util.List;

public class CountdownPersistenceService implements PersistenceService {

    private final Async async;

    public CountdownPersistenceService(Async async) {
        this.async = async;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler) {
        this.store(models.size(), resultHandler);
    }

    @Override
    public void queryAccountMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void queryLiquiGroupMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void queryLiquiGroupSplitMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void queryPoolMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void queryPositionReport(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void queryRiskLimitUtilization(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Query is not implemented"));
    }

    @Override
    public void close() {
    }

    private void store(int count, Handler<AsyncResult<Void>> resultHandler) {

        for (int i = 0; i < count; i++) {
            this.async.countDown();
        }

        // Always succeeds
        resultHandler.handle(Future.succeededFuture());
    }
}
