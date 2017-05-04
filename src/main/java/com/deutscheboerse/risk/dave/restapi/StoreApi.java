package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.ApiVerticle;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import io.vertx.serviceproxy.ProxyHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class StoreApi {
    private static final Logger LOG = LoggerFactory.getLogger(StoreApi.class);
    private static final int DEFAULT_PROXY_SEND_TIMEOUT = 60000;

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public StoreApi(Vertx vertx) {
        this.vertx = vertx;
        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(DEFAULT_PROXY_SEND_TIMEOUT);
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS, deliveryOptions);
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
        JsonArray bodyAsJsonArray = routingContext.getBodyAsJsonArray();
        switch(routingContext.request().getParam("model")) {
            case ApiVerticle.ACCOUNT_MARGIN_REQUEST_PARAMETER:
                List<AccountMarginModel> accountMarginModels = this.getModelsFromJsonArray(bodyAsJsonArray, AccountMarginModel::new);
                this.persistenceProxy.storeAccountMargin(accountMarginModels, this.getResponseHandler(routingContext));
                break;
            case ApiVerticle.LIQUI_GROUP_MARGIN_REQUEST_PARAMETER:
                List<LiquiGroupMarginModel> liquiGroupMarginModels = this.getModelsFromJsonArray(bodyAsJsonArray, LiquiGroupMarginModel::new);
                this.persistenceProxy.storeLiquiGroupMargin(liquiGroupMarginModels, this.getResponseHandler(routingContext));
                break;
            case ApiVerticle.LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER:
                List<LiquiGroupSplitMarginModel> liquiGroupSplitMarginModels = this.getModelsFromJsonArray(bodyAsJsonArray, LiquiGroupSplitMarginModel::new);
                this.persistenceProxy.storeLiquiGroupSplitMargin(liquiGroupSplitMarginModels, this.getResponseHandler(routingContext));
                break;
            case ApiVerticle.POOL_MARGIN_REQUEST_PARAMETER:
                List<PoolMarginModel> poolMarginModels = this.getModelsFromJsonArray(bodyAsJsonArray, PoolMarginModel::new);
                this.persistenceProxy.storePoolMargin(poolMarginModels, this.getResponseHandler(routingContext));
                break;
            case ApiVerticle.POSITION_REPORT_REQUEST_PARAMETER:
                List<PositionReportModel> positionReportModels = this.getModelsFromJsonArray(bodyAsJsonArray, PositionReportModel::new);
                this.persistenceProxy.storePositionReport(positionReportModels, this.getResponseHandler(routingContext));
                break;
            case ApiVerticle.RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER:
                List<RiskLimitUtilizationModel> riskLimitUtilizationModels = this.getModelsFromJsonArray(bodyAsJsonArray, RiskLimitUtilizationModel::new);
                this.persistenceProxy.storeRiskLimitUtilization(riskLimitUtilizationModels, this.getResponseHandler(routingContext));
                break;
            default:
                LOG.error("Unrecognized model type");
                routingContext.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                break;
        }
    }

    private <T extends AbstractModel> List<T> getModelsFromJsonArray(JsonArray jsonArray, Function<JsonObject, T> modelFactory) {
        List<T> models = new ArrayList<>(jsonArray.size());
        jsonArray.forEach(json -> {
            T model = modelFactory.apply((JsonObject) json);
            model.validate();
            models.add(model);
        });
        return models;
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
