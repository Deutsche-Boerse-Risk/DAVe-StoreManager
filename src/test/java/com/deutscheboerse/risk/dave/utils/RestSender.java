package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface RestSender {
    void sendAllData(Handler<AsyncResult<Void>> handler);
    void sendAccountMarginData(Handler<AsyncResult<Void>> handler);
    void sendLiquiGroupMarginData(Handler<AsyncResult<Void>> handler);
    void sendLiquiGroupSplitMarginData(Handler<AsyncResult<Void>> handler);
    void sendPoolMarginData(Handler<AsyncResult<Void>> handler);
    void sendPositionReportData(Handler<AsyncResult<Void>> handler);
    void sendRiskLimitUtilizationData(Handler<AsyncResult<Void>> handler);
}
