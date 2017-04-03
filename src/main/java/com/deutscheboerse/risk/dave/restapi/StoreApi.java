package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.HttpVerticle;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
    }

    public void storeHandler(RoutingContext routingContext) {
        try {
            this.doStore(routingContext);
        } catch (IllegalArgumentException e) {
            LOG.error("Bad request: {}", e.getMessage(), e);
            routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
        }
    }

    private void doStore(RoutingContext routingContext) {
        switch(routingContext.request().getParam("model")) {
            case HttpVerticle.ACCOUNT_MARGIN_REQUEST_PARAMETER:
                AccountMarginModel accountMarginModel = new AccountMarginModel(routingContext.getBodyAsJson());
                accountMarginModel.validate();
                this.persistenceProxy.storeAccountMargin(accountMarginModel, this.getResponseHandler(routingContext));
                break;
            case HttpVerticle.LIQUI_GROUP_MARGIN_REQUEST_PARAMETER:
                LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel(routingContext.getBodyAsJson());
                liquiGroupMarginModel.validate();
                this.persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModel, this.getResponseHandler(routingContext));
                break;
            case HttpVerticle.LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER:
                LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel(routingContext.getBodyAsJson());
                liquiGroupSplitMarginModel.validate();
                this.persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModel, this.getResponseHandler(routingContext));
                break;
            case HttpVerticle.POOL_MARGIN_REQUEST_PARAMETER:
                PoolMarginModel poolMarginModel = new PoolMarginModel(routingContext.getBodyAsJson());
                poolMarginModel.validate();
                this.persistenceProxy.storePoolMargin(poolMarginModel, this.getResponseHandler(routingContext));
                break;
            case HttpVerticle.POSITION_REPORT_REQUEST_PARAMETER:
                PositionReportModel positionReportModel = new PositionReportModel(routingContext.getBodyAsJson());
                positionReportModel.validate();
                this.persistenceProxy.storePositionReport(positionReportModel, this.getResponseHandler(routingContext));
                break;
            case HttpVerticle.RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER:
                RiskLimitUtilizationModel riskLimitUtilizationModel = new RiskLimitUtilizationModel(routingContext.getBodyAsJson());
                riskLimitUtilizationModel.validate();
                this.persistenceProxy.storeRiskLimitUtilization(riskLimitUtilizationModel, this.getResponseHandler(routingContext));
                break;
            default:
                LOG.error("Unrecognized model type");
                routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                break;
        }
    }

    private Handler<AsyncResult<Void>> getResponseHandler(RoutingContext routingContext) {
        return ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for store request");
                routingContext.response()
                        .setStatusCode(HttpResponseStatus.CREATED.code())
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end();
            } else {
                LOG.error("Failed to store the document", ar.cause());
                routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
            }
        };
    }
}
