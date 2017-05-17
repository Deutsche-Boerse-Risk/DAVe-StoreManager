package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.grpc.*;
import com.deutscheboerse.risk.dave.model.Model;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.deutscheboerse.risk.dave.persistence.RequestType;
import com.google.protobuf.MessageLite;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.grpc.GrpcWriteStream;
import io.vertx.serviceproxy.ProxyHelper;

import java.util.List;

public class QueryApi {
    private static final Logger LOG = LoggerFactory.getLogger(QueryApi.class);
    private static final int DEFAULT_PROXY_SEND_TIMEOUT = 60000;

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public QueryApi(Vertx vertx) {
        this.vertx = vertx;
        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(DEFAULT_PROXY_SEND_TIMEOUT);
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS, deliveryOptions);
    }

    public void queryAccountMargin(AccountMarginQuery request, GrpcWriteStream<AccountMargin> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("member", request.getMember());
        query.put("account", request.getAccount());
        query.put("marginCurrency", request.getMarginCurrency());
        query.put("clearingCurrency", request.getClearingCurrency());
        query.put("pool", request.getPool());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryAccountMargin);
    }

    public void queryLiquiGroupMargin(LiquiGroupMarginQuery request, GrpcWriteStream<LiquiGroupMargin> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("member", request.getMember());
        query.put("account", request.getAccount());
        query.put("marginClass", request.getMarginClass());
        query.put("marginCurrency", request.getMarginCurrency());
        query.put("marginGroup", request.getMarginGroup());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryLiquiGroupMargin);
    }

    public void queryLiquiGroupSplitMargin(LiquiGroupSplitMarginQuery request, GrpcWriteStream<LiquiGroupSplitMargin> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("member", request.getMember());
        query.put("account", request.getAccount());
        query.put("liquidationGroup", request.getLiquidationGroup());
        query.put("liquidationGroupSplit", request.getLiquidationGroupSplit());
        query.put("marginCurrency", request.getMarginCurrency());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryLiquiGroupSplitMargin);
    }

    public void queryPoolMargin(PoolMarginQuery request, GrpcWriteStream<PoolMargin> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("pool", request.getPool());
        query.put("marginCurrency", request.getMarginCurrency());
        query.put("clrRptCurrency", request.getClrRptCurrency());
        query.put("poolOwner", request.getPoolOwner());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryPoolMargin);
    }

    public void queryPositionReport(PositionReportQuery request, GrpcWriteStream<PositionReport> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("member", request.getMember());
        query.put("account", request.getAccount());
        query.put("liquidationGroup", request.getLiquidationGroup());
        query.put("liquidationGroupSplit", request.getLiquidationGroupSplit());
        query.put("product", request.getProduct());
        query.put("callPut", request.getCallPut());
        query.put("contractYear", request.getContractYear());
        query.put("contractMonth", request.getContractMonth());
        query.put("expiryDay", request.getExpiryDay());
        query.put("exercisePrice", request.getExercisePrice());
        query.put("version", request.getVersion());
        query.put("flexContractSymbol", request.getFlexContractSymbol());
        query.put("clearingCurrency", request.getClearingCurrency());
        query.put("productCurrency", request.getProductCurrency());
        query.put("underlying", request.getUnderlying());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryPositionReport);
    }

    public void queryRiskLimitUtilization(RiskLimitUtilizationQuery request, GrpcWriteStream<RiskLimitUtilization> response) {
        RequestType requestType = request.getLatest() ? RequestType.LATEST : RequestType.HISTORY;
        JsonObject query = new JsonObject();
        query.put("clearer", request.getClearer());
        query.put("member", request.getMember());
        query.put("maintainer", request.getMaintainer());
        query.put("limitType", request.getLimitType());

        query(requestType, this.getFilteredQueryParams(query), response, this.persistenceProxy::queryRiskLimitUtilization);
    }

    private JsonObject getFilteredQueryParams(JsonObject query) {
        JsonObject result = new JsonObject();
        query.stream()
            .filter(entry -> (!(entry.getValue() instanceof String) || !"*".equals(entry.getValue())))
            .filter(entry -> (!(entry.getValue() instanceof Integer) || !Integer.valueOf(-1).equals(entry.getValue())))
            .filter(entry -> (!(entry.getValue() instanceof Double) || !Double.valueOf(-1.0d).equals(entry.getValue())))
            .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    private interface QueryFunction<T extends MessageLite, U extends Model<T>> {
        void query(RequestType requestType, JsonObject query, Handler<AsyncResult<List<U>>> resultHandler);
    }

    private <T extends MessageLite, U extends Model<T>>
    void query(RequestType requestType,
               JsonObject query,
               GrpcWriteStream<T> response,
               QueryFunction<T, U> queryFunction) {
        queryFunction.query(requestType, query, ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for query request");
                ar.result().forEach(model -> response.write(model.toGrpc()));
                response.end();
            } else {
                LOG.error("Failed to query the DB service", ar.cause());
                response.fail(ar.cause());
            }
        });
    }
}
