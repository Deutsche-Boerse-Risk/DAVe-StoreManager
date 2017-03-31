package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.serviceproxy.ServiceException;

public class CountdownPersistenceService implements PersistenceService {

    private final Async async;
    private JsonObject lastMessage;

    public CountdownPersistenceService(Async async) {
        this.async = async;
    }

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        this.store(model, resultHandler);
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

    private void store(JsonObject message, Handler<AsyncResult<Void>> resultHandler) {

        // Store the message
        this.lastMessage = message;

        this.async.countDown();

        // Always succeeds
        resultHandler.handle(Future.succeededFuture());
    }

    public JsonObject getLastMessage() {
        return this.lastMessage;
    }
}
