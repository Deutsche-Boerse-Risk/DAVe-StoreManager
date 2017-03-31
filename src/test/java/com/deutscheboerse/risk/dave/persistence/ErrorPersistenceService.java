package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceException;

public class ErrorPersistenceService implements PersistenceService {

    @Override
    public void initialize(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(ServiceException.fail(STORE_ERROR, "Unable to store message"));
    }

    @Override
    public void queryAccountMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void queryLiquiGroupMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void queryLiquiGroupSplitMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void queryPoolMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void queryPositionReport(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void queryRiskLimitUtilization(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler) {
        resultHandler.handle(ServiceException.fail(QUERY_ERROR, "Unable to query message"));
    }

    @Override
    public void close() {
    }
}
