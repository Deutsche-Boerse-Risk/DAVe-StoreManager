package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;

public class StoreApi {
    private static final Logger LOG = LoggerFactory.getLogger(StoreApi.class);

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public StoreApi(Vertx vertx) {
        this.vertx = vertx;
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);;
    }

    public void storeAccountMarginHandler(RoutingContext routingContext) {
        AccountMarginModel model = new AccountMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storeAccountMargin(model, this.getResponseHandler(routingContext));
    }

    public void storeLiquiGroupMarginHandler(RoutingContext routingContext) {
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storeLiquiGroupMargin(model, this.getResponseHandler(routingContext));
    }

    public void storeLiquiGroupSplitMarginHandler(RoutingContext routingContext) {
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storeLiquiGroupSplitMargin(model, this.getResponseHandler(routingContext));
    }

    public void storePoolMarginHandler(RoutingContext routingContext) {
        PoolMarginModel model = new PoolMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storePoolMargin(model, this.getResponseHandler(routingContext));
    }

    public void storePositionReportHandler(RoutingContext routingContext) {
        PositionReportModel model = new PositionReportModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storePositionReport(model, this.getResponseHandler(routingContext));
    }

    public void storeRiskLimitUtilizationHandler(RoutingContext routingContext) {
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(routingContext.getBodyAsJson());
        this.persistenceProxy.storeRiskLimitUtilization(model, this.getResponseHandler(routingContext));
    }

    private Handler<AsyncResult<Void>> getResponseHandler(RoutingContext routingContext) {
        return ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for store request");
                routingContext.response()
                        .setStatusCode(HttpResponseStatus.CREATED.code())
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(new JsonObject().put("status", "ok").encode());
            } else {
                LOG.error("Failed to store the document", ar.cause());
                routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
            }
        };
    }
}
