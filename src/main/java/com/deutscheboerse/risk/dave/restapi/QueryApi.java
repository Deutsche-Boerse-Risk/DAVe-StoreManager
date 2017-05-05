package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.ApiVerticle;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.persistence.RequestType;
import com.google.common.base.Preconditions;
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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class QueryApi {
    private static final Logger LOG = LoggerFactory.getLogger(QueryApi.class);

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public QueryApi(Vertx vertx) {
        this.vertx = vertx;
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
    }

    public void queryLatestHandler(RoutingContext routingContext) {
        this.doQuery(routingContext, RequestType.LATEST);
    }

    public void queryHistoryHandler(RoutingContext routingContext) {
        this.doQuery(routingContext, RequestType.HISTORY);
    }

    private void doQuery(RoutingContext routingContext, RequestType requestType) {
        try {
            this.queryHandler(routingContext, requestType);
        } catch (IllegalArgumentException e) {
            LOG.error("Bad request: {}", e.getMessage(), e);
            routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
        }
    }

    private void queryHandler(RoutingContext routingContext, RequestType requestType) {
        switch(routingContext.request().getParam("model")) {
            case ApiVerticle.ACCOUNT_MARGIN_REQUEST_PARAMETER:
                AccountMarginModel accountMarginModel = new AccountMarginModel();
                this.persistenceProxy.queryAccountMargin(requestType, this.createParamsFromContext(routingContext, accountMarginModel), getResponseHandler(routingContext));
                break;
            case ApiVerticle.LIQUI_GROUP_MARGIN_REQUEST_PARAMETER:
                LiquiGroupMarginModel liquiGroupMarginModel = new LiquiGroupMarginModel();
                this.persistenceProxy.queryLiquiGroupMargin(requestType, this.createParamsFromContext(routingContext, liquiGroupMarginModel), getResponseHandler(routingContext));
                break;
            case ApiVerticle.LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER:
                LiquiGroupSplitMarginModel liquiGroupSplitMarginModel = new LiquiGroupSplitMarginModel();
                this.persistenceProxy.queryLiquiGroupSplitMargin(requestType, this.createParamsFromContext(routingContext, liquiGroupSplitMarginModel), getResponseHandler(routingContext));
                break;
            case ApiVerticle.POOL_MARGIN_REQUEST_PARAMETER:
                PoolMarginModel poolMarginModel = new PoolMarginModel();
                this.persistenceProxy.queryPoolMargin(requestType, this.createParamsFromContext(routingContext, poolMarginModel), getResponseHandler(routingContext));
                break;
            case ApiVerticle.POSITION_REPORT_REQUEST_PARAMETER:
                PositionReportModel positionReportModel = new PositionReportModel();
                this.persistenceProxy.queryPositionReport(requestType, this.createParamsFromContext(routingContext, positionReportModel), getResponseHandler(routingContext));
                break;
            case ApiVerticle.RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER:
                RiskLimitUtilizationModel riskLimitUtilizationModel = new RiskLimitUtilizationModel();
                this.persistenceProxy.queryRiskLimitUtilization(requestType, this.createParamsFromContext(routingContext, riskLimitUtilizationModel), getResponseHandler(routingContext));
                break;
            default:
                LOG.error("Unrecognized model type");
                routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                break;
        }
    }

    private JsonObject createParamsFromContext(RoutingContext routingContext, AbstractModel model) {
        final JsonObject result = new JsonObject();
        routingContext.request().params().entries()
                .stream()
                .filter(entry -> !"model".equals(entry.getKey()))
                .forEach(entry -> {
                    final String parameterName = entry.getKey();
                    final String parameterValue = entry.getValue();
                    try {
                        String decodedValue = URLDecoder.decode(parameterValue, StandardCharsets.UTF_8.toString());
                        Class<?> parameterType = getParameterType(parameterName, model);
                        result.put(parameterName, convertValue(decodedValue, parameterType));
                    } catch (UnsupportedEncodingException e) {
                        throw new AssertionError(e);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("Cannot convert '%s' (%s) to %s",
                                parameterName, parameterValue, getParameterType(parameterName, model).getSimpleName()));
                    }
                });
        return result;
    }

    private Class<?> getParameterType(String parameterName, AbstractModel model) {
        Map<String, Class> parameterDescriptor = new HashMap<>(model.getKeysDescriptor());
        parameterDescriptor.putAll(model.getUniqueFieldsDescriptor());

        Preconditions.checkArgument(parameterDescriptor.containsKey(parameterName),
                "Unknown parameter '%s'", parameterName);
        return parameterDescriptor.get(parameterName);
    }

    private <T> T convertValue(String value, Class<T> clazz) {
        if (clazz.equals(String.class)) {
            return clazz.cast(value);
        } else if (clazz.equals(Integer.class)) {
            return clazz.cast(Integer.parseInt(value));
        } else if (clazz.equals(Double.class)) {
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
                        .setStatusCode(HttpResponseStatus.OK.code())
                        .putHeader("content-type", "application/json; charset=utf-8")
                        .end(ar.result());
            } else {
                LOG.error("Failed to query the DB service", ar.cause());
                routingContext.response().setStatusCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code()).end();
            }
        };
    }
}
