package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.grpc.*;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.protobuf.MessageLite;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.grpc.GrpcReadStream;
import io.vertx.serviceproxy.ProxyHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
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

    public void storeAccountMargin(GrpcReadStream<AccountMargin> request, Future<StoreReply> response) {
        store(request, response, AccountMarginModel::new, this.persistenceProxy::storeAccountMargin);
    }

    public void storeLiquiGroupMargin(GrpcReadStream<LiquiGroupMargin> request, Future<StoreReply> response) {
        store(request, response, LiquiGroupMarginModel::new, this.persistenceProxy::storeLiquiGroupMargin);
    }

    public void storeLiquiGroupSplitMargin(GrpcReadStream<LiquiGroupSplitMargin> request, Future<StoreReply> response) {
        store(request, response, LiquiGroupSplitMarginModel::new, this.persistenceProxy::storeLiquiGroupSplitMargin);
    }

    public void storePoolMargin(GrpcReadStream<PoolMargin> request, Future<StoreReply> response) {
        store(request, response, PoolMarginModel::new, this.persistenceProxy::storePoolMargin);
    }

    public void storePositionReport(GrpcReadStream<PositionReport> request, Future<StoreReply> response) {
        store(request, response, PositionReportModel::new, this.persistenceProxy::storePositionReport);
    }

    public void storeRiskLimitUtilization(GrpcReadStream<RiskLimitUtilization> request, Future<StoreReply> response) {
        store(request, response, RiskLimitUtilizationModel::new, this.persistenceProxy::storeRiskLimitUtilization);
    }

    private <T extends MessageLite, U extends Model>
    void store(GrpcReadStream<T> request,
               Future<StoreReply> response,
               Function<T, U> modelFactory,
               BiConsumer<List<U>, Handler<AsyncResult<Void>>> storeFunction) {
        List<U> models = new ArrayList<>();
        request.handler(payload -> {
            U model = modelFactory.apply(payload);
            models.add(model);
        }).endHandler(v -> storeFunction.accept(models, getResponseHandler(response))).exceptionHandler(response::fail);
    }

    private Handler<AsyncResult<Void>> getResponseHandler(Future<StoreReply> response) {
        return ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for store request");
                response.complete(StoreReply.newBuilder().build());
            } else {
                LOG.error("Failed to store the document", ar.cause());
                response.fail(ar.cause());
            }
        };
    }
}
