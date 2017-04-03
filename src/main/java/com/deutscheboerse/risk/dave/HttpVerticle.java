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

public class HttpVerticle extends AbstractVerticle
{
    private static final Logger LOG = LoggerFactory.getLogger(HttpVerticle.class);

    private static final Integer DEFAULT_PORT = 8080;

    private static final String API_VERSION = "v1.0";
    public static final String API_PREFIX = String.format("/api/%s", API_VERSION);

    public static final String ACCOUNT_MARGIN_REQUEST_PARAMETER = "am";
    public static final String LIQUI_GROUP_MARGIN_REQUEST_PARAMETER = "lgm";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER = "lgsm";
    public static final String POOL_MARGIN_REQUEST_PARAMETER = "pm";
    public static final String POSITION_REPORT_REQUEST_PARAMETER = "pr";
    public static final String RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER = "rlu";

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
        router.post(String.format("%s/store/:model", API_PREFIX)).handler(storeApi::storeHandler);

        // Query API
        QueryApi queryApi = new QueryApi(vertx);
        router.get(String.format("%s/query/:model/latest", API_PREFIX)).handler(queryApi::queryLatestHandler);
        router.get(String.format("%s/query/:model/history", API_PREFIX)).handler(queryApi::queryHistoryHandler);

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
