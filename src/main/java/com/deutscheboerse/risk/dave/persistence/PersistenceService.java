package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.codegen.annotations.ProxyClose;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import java.util.List;

@ProxyGen
public interface PersistenceService {
    String SERVICE_ADDRESS = "persistenceService";

    int INIT_ERROR = 2;
    int STORE_ERROR = 3;
    int QUERY_ERROR = 4;

    void initialize(Handler<AsyncResult<Void>> resultHandler);

    void storeAccountMargin(List<AccountMarginModel> models, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupMargin(List<LiquiGroupMarginModel> models, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupSplitMargin(List<LiquiGroupSplitMarginModel> models, Handler<AsyncResult<Void>> resultHandler);
    void storePoolMargin(List<PoolMarginModel> models, Handler<AsyncResult<Void>> resultHandler);
    void storePositionReport(List<PositionReportModel> models, Handler<AsyncResult<Void>> resultHandler);
    void storeRiskLimitUtilization(List<RiskLimitUtilizationModel> models, Handler<AsyncResult<Void>> resultHandler);

    void queryAccountMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    void queryLiquiGroupMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    void queryLiquiGroupSplitMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    void queryPoolMargin(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    void queryPositionReport(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);
    void queryRiskLimitUtilization(RequestType type, JsonObject query, Handler<AsyncResult<String>> resultHandler);

    @ProxyClose
    void close();
}
