package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.deutscheboerse.risk.dave.restapi.QueryApi;
import com.deutscheboerse.risk.dave.restapi.StoreApi;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import static com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component.HTTP;

public class HttpVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(HttpVerticle.class);

    private static final Integer DEFAULT_PORT = 8080;
    private static final Boolean DEFAULT_SSL_REQUIRE_CLIENT_AUTH = false;

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
        JsonObject configWithoutSensitiveInfo = config().copy()
                .put("sslKey", "******************")
                .put("sslCert", "******************");
        JsonArray trustCertsWithoutSensitiveInfo = new JsonArray();
        config().getJsonArray("sslTrustCerts").forEach(key -> trustCertsWithoutSensitiveInfo.add("******************"));
        configWithoutSensitiveInfo.put("sslTrustCerts", new JsonArray().add("******************"));
        LOG.info("Starting {} with configuration: {}", HttpVerticle.class.getSimpleName(), configWithoutSensitiveInfo.encodePrettily());

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
        HttpServerOptions httpServerOptions = this.createHttpServerOptions();
        server = vertx.createHttpServer(httpServerOptions)
                .requestHandler(router::accept)
                .listen(port, webServerFuture.completer());

        return webServerFuture;
    }

    private HttpServerOptions createHttpServerOptions() {
        HttpServerOptions httpOptions = new HttpServerOptions();
        this.setSSL(httpOptions);
        return httpOptions;
    }

    private void setSSL(HttpServerOptions httpServerOptions) {
        httpServerOptions.setSsl(true);
        PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
                .setKeyValue(Buffer.buffer(config().getString("sslKey")))
                .setCertValue(Buffer.buffer(config().getString("sslCert")));
        httpServerOptions.setPemKeyCertOptions(pemKeyCertOptions);

        PemTrustOptions pemTrustOptions = new PemTrustOptions();
        config().getJsonArray("sslTrustCerts", new JsonArray())
                .stream()
                .map(Object::toString)
                .forEach(trustKey -> pemTrustOptions.addCertValue(Buffer.buffer(trustKey)));
        if (!pemTrustOptions.getCertValues().isEmpty()) {
            httpServerOptions.setPemTrustOptions(pemTrustOptions);
            ClientAuth clientAuth = config().getBoolean("sslRequireClientAuth", DEFAULT_SSL_REQUIRE_CLIENT_AUTH) ?
                    ClientAuth.REQUIRED : ClientAuth.REQUEST;
            httpServerOptions.setClientAuth(clientAuth);
        }
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
