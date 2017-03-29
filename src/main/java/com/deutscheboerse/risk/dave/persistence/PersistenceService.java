package com.deutscheboerse.risk.dave.persistence;

import com.deutscheboerse.risk.dave.model.*;
import io.vertx.codegen.annotations.ProxyClose;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

@ProxyGen
public interface PersistenceService {
    String SERVICE_ADDRESS = "persistenceService";

    int INIT_ERROR = 2;
    int STORE_ERROR = 3;

    void initialize(Handler<AsyncResult<Void>> resultHandler);

    void storeAccountMargin(AccountMarginModel model, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupMargin(LiquiGroupMarginModel model, Handler<AsyncResult<Void>> resultHandler);
    void storeLiquiGroupSplitMargin(LiquiGroupSplitMarginModel model, Handler<AsyncResult<Void>> resultHandler);
    void storePoolMargin(PoolMarginModel model, Handler<AsyncResult<Void>> resultHandler);
    void storePositionReport(PositionReportModel model, Handler<AsyncResult<Void>> resultHandler);
    void storeRiskLimitUtilization(RiskLimitUtilizationModel model, Handler<AsyncResult<Void>> resultHandler);

    @ProxyClose
    void close();
}
