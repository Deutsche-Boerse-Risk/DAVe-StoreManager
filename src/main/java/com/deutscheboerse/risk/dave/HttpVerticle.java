package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.restapi.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import static com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component.HTTP;

/**
 * Starts an {@link HttpServer} on default port 8080.
 * <p>
 * It exports these two web services:
 * <ul>
 *   <li>/healthz   - Always replies "ok" (provided the web server is running)
 *   <li>/readiness - Replies "ok" or "nok" indicating whether all verticles
 *                    are up and running
 * </ul>
 */
public class HttpVerticle extends AbstractVerticle
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpVerticle.class);

    private static final Integer DEFAULT_PORT = 8080;

    private static final String API_VERSION = "v1.0";
    private static final String API_PREFIX = String.format("/api/%s", API_VERSION);

    public static final String STORE_ACCOUNT_MARGIN_API = String.format("%s/store/am", API_PREFIX);
    public static final String STORE_LIQUI_GROUP_MARGIN_API = String.format("%s/store/lgm", API_PREFIX);
    public static final String STORE_LIQUI_GROUP_SPLIT_MARGIN_API = String.format("%s/store/lgsm", API_PREFIX);
    public static final String STORE_POOL_MARGIN_API = String.format("%s/store/pm", API_PREFIX);
    public static final String STORE_POSITION_REPORT_API = String.format("%s/store/pr", API_PREFIX);
    public static final String STORE_RISK_LIMIT_UTILIZATION_API = String.format("%s/store/rlu", API_PREFIX);

    public static final String QUERY_ACCOUNT_MARGIN_API = String.format("%s/query/am", API_PREFIX);
    public static final String QUERY_LIQUI_GROUP_MARGIN_API = String.format("%s/query/lgm", API_PREFIX);
    public static final String QUERY_LIQUI_GROUP_SPLIT_MARGIN_API = String.format("%s/query/lgsm", API_PREFIX);
    public static final String QUERY_POOL_MARGIN_API = String.format("%s/query/pm", API_PREFIX);
    public static final String QUERY_POSITION_REPORT_API = String.format("%s/query/pr", API_PREFIX);
    public static final String QUERY_RISK_LIMIT_UTILIZATION_API = String.format("%s/query/rlu", API_PREFIX);

    public static final String REST_HEALTHZ = "/healthz";
    public static final String REST_READINESS = "/readiness";

    private HttpServer server;
    private HealthCheck healthCheck;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        LOG.info("Starting {} with configuration: {}", HttpVerticle.class.getSimpleName(), config().encodePrettily());

        healthCheck = new HealthCheck(this.vertx);

        startHttpServer().setHandler(ar -> {
            if (ar.succeeded()) {
                healthCheck.setComponentReady(HTTP);
                startFuture.complete();
            }
            else {
                healthCheck.setComponentFailed(HTTP);
                startFuture.fail(ar.cause());
            }
        });
    }

    private Future<HttpServer> startHttpServer() {
        Future<HttpServer> webServerFuture = Future.future();
        Router router = configureRouter();

        int port = config().getInteger("port", HttpVerticle.DEFAULT_PORT);

        LOG.info("Starting web server on port {}", port);
        server = vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(port, webServerFuture.completer());

        return webServerFuture;
    }

    private Router configureRouter() {
        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);
        HealthCheckHandler readinessHandler = HealthCheckHandler.create(vertx);

        healthCheckHandler.register("healthz", this::healthz);
        readinessHandler.register("readiness", this::readiness);

        Router router = Router.router(vertx);

        LOG.info("Adding route REST API");
        router.route(String.format("%s/*", API_PREFIX)).handler(BodyHandler.create());
        
        // HealthCheck API
        router.get(REST_HEALTHZ).handler(healthCheckHandler);
        router.get(REST_READINESS).handler(readinessHandler);
        
        // Store API
        StoreApi storeApi = new StoreApi(vertx);
        router.post(STORE_ACCOUNT_MARGIN_API).handler(storeApi::storeAccountMarginHandler);
        router.post(STORE_LIQUI_GROUP_MARGIN_API).handler(storeApi::storeLiquiGroupMarginHandler);
        router.post(STORE_LIQUI_GROUP_SPLIT_MARGIN_API).handler(storeApi::storeLiquiGroupSplitMarginHandler);
        router.post(STORE_POOL_MARGIN_API).handler(storeApi::storePoolMarginHandler);
        router.post(STORE_POSITION_REPORT_API).handler(storeApi::storePositionReportHandler);
        router.post(STORE_RISK_LIMIT_UTILIZATION_API).handler(storeApi::storeRiskLimitUtilizationHandler);

        // Query API
        QueryApi queryApi = new QueryApi(vertx);
        router.get(String.format("%s/latest", QUERY_ACCOUNT_MARGIN_API)).handler(queryApi::queryLatestAccountMarginHandler);
        router.get(String.format("%s/latest", QUERY_LIQUI_GROUP_MARGIN_API)).handler(queryApi::queryLatestLiquiGroupMarginHandler);
        router.get(String.format("%s/latest", QUERY_LIQUI_GROUP_SPLIT_MARGIN_API)).handler(queryApi::queryLatestLiquiGroupSplitMarginHandler);
        router.get(String.format("%s/latest", QUERY_POOL_MARGIN_API)).handler(queryApi::queryLatestPoolMarginHandler);
        router.get(String.format("%s/latest", QUERY_POSITION_REPORT_API)).handler(queryApi::queryLatestPositionReportHandler);
        router.get(String.format("%s/latest", QUERY_RISK_LIMIT_UTILIZATION_API)).handler(queryApi::queryLatestRiskLimitUtilizationHandler);

        router.get(String.format("%s/history", QUERY_ACCOUNT_MARGIN_API)).handler(queryApi::queryHistoryAccountMarginHandler);
        router.get(String.format("%s/history", QUERY_LIQUI_GROUP_MARGIN_API)).handler(queryApi::queryHistoryLiquiGroupMarginHandler);
        router.get(String.format("%s/history", QUERY_LIQUI_GROUP_SPLIT_MARGIN_API)).handler(queryApi::queryHistoryLiquiGroupSplitMarginHandler);
        router.get(String.format("%s/history", QUERY_POOL_MARGIN_API)).handler(queryApi::queryHistoryPoolMarginHandler);
        router.get(String.format("%s/history", QUERY_POSITION_REPORT_API)).handler(queryApi::queryHistoryPositionReportHandler);
        router.get(String.format("%s/history", QUERY_RISK_LIMIT_UTILIZATION_API)).handler(queryApi::queryHistoryRiskLimitUtilizationHandler);

        return router;
    }

    private void healthz(Future<Status> future) {
        future.complete(Status.OK());
    }

    private void readiness(Future<Status> future) {
        future.complete(healthCheck.ready() ? Status.OK() : Status.KO());
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Shutting down webserver");
        server.close();
    }
}
