package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.PersistenceServiceGrpc;
import com.deutscheboerse.risk.dave.StoreReply;
import com.deutscheboerse.risk.dave.model.*;
import com.google.protobuf.MessageLite;
import io.grpc.ManagedChannel;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.TCPSSLOptions;
import io.vertx.grpc.GrpcUniExchange;
import io.vertx.grpc.VertxChannelBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GrpcSenderRegular implements GrpcSender {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcSenderRegular.class);

    private final Vertx vertx;

    public GrpcSenderRegular(Vertx vertx) {
        this.vertx = vertx;
    }

    public void sendAllData(Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();

        Future<Void> accountMarginFuture = Future.future();
        Future<Void> liquiGroupMarginFuture = Future.future();
        Future<Void> liquiGroupSplitMarginFuture = Future.future();
        Future<Void> poolMarginFuture = Future.future();
        Future<Void> positionReportFuture = Future.future();
        Future<Void> riskLimitUtilizationFuture = Future.future();

        this.sendAccountMarginData(accountMarginFuture);
        this.sendLiquiGroupMarginData(liquiGroupMarginFuture);
        this.sendLiquiGroupSplitMarginData(liquiGroupSplitMarginFuture);
        this.sendPoolMarginData(poolMarginFuture);
        this.sendPositionReportData(positionReportFuture);
        this.sendRiskLimitUtilizationData(riskLimitUtilizationFuture);

        futures.add(accountMarginFuture);
        futures.add(liquiGroupMarginFuture);
        futures.add(liquiGroupSplitMarginFuture);
        futures.add(poolMarginFuture);
        futures.add(positionReportFuture);
        futures.add(riskLimitUtilizationFuture);

        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void sendAccountMarginData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeAccountMargin, AccountMarginModel::buildFromJson, DataHelper.ACCOUNT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupMarginData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeLiquiGroupMargin, LiquiGroupMarginModel::buildFromJson, DataHelper.LIQUI_GROUP_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupSplitMarginData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeLiquiGroupSplitMargin, LiquiGroupSplitMarginModel::buildFromJson, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPoolMarginData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storePoolMargin, PoolMarginModel::buildFromJson, DataHelper.POOL_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPositionReportData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storePositionReport, PositionReportModel::buildFromJson, DataHelper.POSITION_REPORT_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendRiskLimitUtilizationData(Handler<AsyncResult<Void>> handler) {
        ManagedChannel channel = this.createGrpcChannel();
        this.sendData(channel, PersistenceServiceGrpc.newVertxStub(channel)::storeRiskLimitUtilization, RiskLimitUtilizationModel::buildFromJson, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    private Handler<AsyncResult<Void>> getResponseHandler(Handler<AsyncResult<Void>> handler) {
        return ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
    }

    private <U extends MessageLite>
    Future<Void> sendData(ManagedChannel channel, Consumer<Handler<GrpcUniExchange<U, StoreReply>>> storeFunction, Function<JsonObject, Model<U>> grpcFunction, String folderName) {
        Future<Void> resultFuture = Future.future();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 2)
                .boxed()
                .collect(Collectors.toList());
        vertx.executeBlocking(future -> {
            CountDownLatch countDownLatch = new CountDownLatch(ttsaveNumbers.size());
            ttsaveNumbers.forEach(ttsaveNo -> this.sendModels(storeFunction, grpcFunction, folderName, ttsaveNo, res -> {
                if (res.succeeded()) {
                    countDownLatch.countDown();
                }
            }));
            try {
                countDownLatch.await(30, TimeUnit.SECONDS);
                future.complete();
            } catch (InterruptedException e) {
                future.fail(e.getCause());
            }
        }, resultFuture);
        resultFuture.setHandler(v -> channel.shutdown());
        return resultFuture;
    }

    protected <U extends MessageLite>
    void sendModels(Consumer<Handler<GrpcUniExchange<U, StoreReply>>> storeFunction, Function<JsonObject, Model<U>> grpcFunction, String folderName, int ttsaveNo, Handler<AsyncResult<Void>> resultHandler) {
        storeFunction.accept(exchange -> { exchange
                .handler(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        LOG.error("Service unavailable", ar);
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });

            DataHelper.readTTSaveFile(folderName, ttsaveNo).forEach(json -> {
                U grpcModel = grpcFunction.apply(json).toGrpc();
                exchange.write(grpcModel);
            });
            exchange.end();
        });
    }

    private ManagedChannel createGrpcChannel() {
        return VertxChannelBuilder
                .forAddress(vertx, "localhost", TestConfig.API_PORT)
                .useSsl(this::setGrpcSslOptions)
                .build();
    }

    private void setGrpcSslOptions(TCPSSLOptions sslOptions) {
        sslOptions
                .setSsl(true)
                .setUseAlpn(true)
                .setPemTrustOptions(TestConfig.API_SERVER_CERTIFICATE.trustOptions())
                .setPemKeyCertOptions(TestConfig.API_CLIENT_CERTIFICATE.keyCertOptions());
    }

}
