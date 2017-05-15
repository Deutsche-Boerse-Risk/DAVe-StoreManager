package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.config.ApiConfig;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.restapi.QueryApi;
import com.deutscheboerse.risk.dave.restapi.StoreApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.TCPSSLOptions;
import io.vertx.grpc.GrpcReadStream;
import io.vertx.grpc.GrpcWriteStream;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component.API;

public class ApiVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(ApiVerticle.class);

    private static final String HIDDEN_CERTIFICATE = "******************";

    private VertxServer server;
    private ApiConfig config;

    static {
        // Disable grpc info logs
        java.util.logging.Logger grpcLogger = java.util.logging.Logger.getLogger("io.grpc");
        grpcLogger.setLevel(java.util.logging.Level.WARNING);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        LOG.info("Starting {} with configuration: {}", ApiVerticle.class.getSimpleName(), hideCertificates(config()).encodePrettily());

        config = (new ObjectMapper()).readValue(config().toString(), ApiConfig.class);

        HealthCheck healthCheck = new HealthCheck(this.vertx);

        startRpcServer().setHandler(ar -> {
            if (ar.succeeded()) {
                healthCheck.setComponentReady(API);
                startFuture.complete();
            }
            else {
                healthCheck.setComponentFailed(API);
                startFuture.fail(ar.cause());
            }
        });
    }

    private JsonObject hideCertificates(JsonObject config) {
        return config.copy()
                .put("sslKey", HIDDEN_CERTIFICATE)
                .put("sslCert", HIDDEN_CERTIFICATE)
                .put("sslTrustCerts", new JsonArray(
                        config.getJsonArray("sslTrustCerts").stream()
                                .map(i -> HIDDEN_CERTIFICATE).collect(Collectors.toList()))
                );
    }

    private Future<Void> startRpcServer() {
        Future<Void> rpcServerFuture = Future.future();
        int port = config.getPort();

        this.server = VertxServerBuilder
                .forPort(vertx, port)
                .useSsl(this::setSslOptions)
                .addService(this.getService())
                .build();

        LOG.info("Starting gRPC server on port {}", port);
        this.server.start(rpcServerFuture);
        return rpcServerFuture;
    }

    private void setSslOptions(TCPSSLOptions sslOptions) {
        sslOptions.setSsl(true);
        sslOptions.setUseAlpn(true);
        PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
                .setKeyValue(Buffer.buffer(config.getSslKey()))
                .setCertValue(Buffer.buffer(config.getSslCert()));
        sslOptions.setPemKeyCertOptions(pemKeyCertOptions);
        PemTrustOptions pemTrustOptions = new PemTrustOptions();
        Arrays.stream(config.getSslTrustCerts())
                .map(Object::toString)
                .forEach(trustKey -> pemTrustOptions.addCertValue(Buffer.buffer(trustKey)));
        if (!pemTrustOptions.getCertValues().isEmpty()) {
            sslOptions.setPemTrustOptions(pemTrustOptions);
        }
    }

    private PersistenceServiceGrpc.PersistenceServiceVertxImplBase getService() {
        return new PersistenceServiceGrpc.PersistenceServiceVertxImplBase() {
            private StoreApi storeApi = new StoreApi(vertx);
            private QueryApi queryApi = new QueryApi(vertx);

            @Override
            public void storeAccountMargin(GrpcReadStream<AccountMargin> request, Future<StoreReply> response) {
                storeApi.storeAccountMargin(request, response);
            }

            @Override
            public void storeLiquiGroupMargin(GrpcReadStream<LiquiGroupMargin> request, Future<StoreReply> response) {
                storeApi.storeLiquiGroupMargin(request, response);
            }

            @Override
            public void storeLiquiGroupSplitMargin(GrpcReadStream<LiquiGroupSplitMargin> request, Future<StoreReply> response) {
                storeApi.storeLiquiGroupSplitMargin(request, response);
            }

            @Override
            public void storePoolMargin(GrpcReadStream<PoolMargin> request, Future<StoreReply> response) {
                storeApi.storePoolMargin(request, response);
            }

            @Override
            public void storePositionReport(GrpcReadStream<PositionReport> request, Future<StoreReply> response) {
                storeApi.storePositionReport(request, response);
            }

            @Override
            public void storeRiskLimitUtilization(GrpcReadStream<RiskLimitUtilization> request, Future<StoreReply> response) {
                storeApi.storeRiskLimitUtilization(request, response);
            }

            @Override
            public void queryAccountMargin(AccountMarginQuery request, GrpcWriteStream<AccountMargin> response) {
                queryApi.queryAccountMargin(request, response);
            }

            @Override
            public void queryLiquiGroupMargin(LiquiGroupMarginQuery request, GrpcWriteStream<LiquiGroupMargin> response) {
                queryApi.queryLiquiGroupMargin(request, response);
            }

            @Override
            public void queryLiquiGroupSplitMargin(LiquiGroupSplitMarginQuery request, GrpcWriteStream<LiquiGroupSplitMargin> response) {
                queryApi.queryLiquiGroupSplitMargin(request, response);
            }

            @Override
            public void queryPoolMargin(PoolMarginQuery request, GrpcWriteStream<PoolMargin> response) {
                queryApi.queryPoolMargin(request, response);
            }

            @Override
            public void queryPositionReport(PositionReportQuery request, GrpcWriteStream<PositionReport> response) {
                queryApi.queryPositionReport(request, response);
            }

            @Override
            public void queryRiskLimitUtilization(RiskLimitUtilizationQuery request, GrpcWriteStream<RiskLimitUtilization> response) {
                queryApi.queryRiskLimitUtilization(request, response);
            }
        };
    }

    @Override
    public void stop() {
        LOG.info("Shutting down gRPC verticle");
        server.shutdown();
    }
}
