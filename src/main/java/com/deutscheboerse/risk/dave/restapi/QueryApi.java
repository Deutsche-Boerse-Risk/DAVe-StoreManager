package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.persistence.RequestType;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class QueryApi {
    private static final Logger LOG = LoggerFactory.getLogger(QueryApi.class);

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public QueryApi(Vertx vertx) {
        this.vertx = vertx;
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
    }

    public void queryLatestAccountMarginHandler(RoutingContext routingContext) {
        AccountMarginModel model = new AccountMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryAccountMargin(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryAccountMarginHandler(RoutingContext routingContext) {
        AccountMarginModel model = new AccountMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryAccountMargin(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryLatestLiquiGroupMarginHandler(RoutingContext routingContext) {
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryLiquiGroupMargin(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryLiquiGroupMarginHandler(RoutingContext routingContext) {
        LiquiGroupMarginModel model = new LiquiGroupMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryLiquiGroupMargin(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryLatestLiquiGroupSplitMarginHandler(RoutingContext routingContext) {
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryLiquiGroupSplitMargin(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryLiquiGroupSplitMarginHandler(RoutingContext routingContext) {
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryLiquiGroupSplitMargin(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryLatestPoolMarginHandler(RoutingContext routingContext) {
        PoolMarginModel model = new PoolMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryPoolMargin(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryPoolMarginHandler(RoutingContext routingContext) {
        PoolMarginModel model = new PoolMarginModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryPoolMargin(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryLatestPositionReportHandler(RoutingContext routingContext) {
        PositionReportModel model = new PositionReportModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryPositionReport(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryPositionReportHandler(RoutingContext routingContext) {
        PositionReportModel model = new PositionReportModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryPositionReport(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryLatestRiskLimitUtilizationHandler(RoutingContext routingContext) {
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryRiskLimitUtilization(RequestType.LATEST, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    public void queryHistoryRiskLimitUtilizationHandler(RoutingContext routingContext) {
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel(routingContext.getBodyAsJson());
        this.persistenceProxy.queryRiskLimitUtilization(RequestType.HISTORY, this.createParamsFromContext(routingContext, model), getResponseHandler(routingContext));
    }

    private JsonObject createParamsFromContext(RoutingContext routingContext, AbstractModel model) {
        final JsonObject result = new JsonObject();
        routingContext.request().params().entries()
                .forEach(entry -> {
                    try {
                        String parameterValue = URLDecoder.decode(entry.getValue(), "UTF-8");
                        Class<?> convertTo = model.getKeysDescriptor().get(entry.getKey());
                        result.put(entry.getKey(), convertValue(parameterValue, convertTo));
                    } catch (UnsupportedEncodingException e) {
                        throw new AssertionError(e);
                    }
                });
        return result;
    }

    private <T> T convertValue(String value, Class<T> clazz) {
        if (clazz == String.class) {
            return clazz.cast(value);
        } else if (clazz == Integer.class) {
            return clazz.cast(Integer.parseInt(value));
        } else if (clazz == Double.class) {
            return clazz.cast(Double.parseDouble(value));
        } else {
            throw new AssertionError("Unsupported type " + clazz);
        }
    }

    private Handler<AsyncResult<String>> getResponseHandler(RoutingContext routingContext) {
        return ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for query request");
                routingContext.response()
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(ar.result());
            } else {
                LOG.error("Failed to query the DB service", ar.cause());
                routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
            }
        };
    }
}
