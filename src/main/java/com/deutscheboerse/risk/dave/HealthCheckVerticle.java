package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.config.HealthCheckConfig;
import com.deutscheboerse.risk.dave.healthcheck.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;

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
public class HealthCheckVerticle extends AbstractVerticle
{
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckVerticle.class);

    public static final String REST_HEALTHZ = "/healthz";
    public static final String REST_READINESS = "/readiness";

    private HttpServer server;
    private HealthCheck healthCheck;
    private HealthCheckConfig config;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        LOG.info("Starting {} with configuration: {}", HealthCheckVerticle.class.getSimpleName(), config().encodePrettily());

        config = (new ObjectMapper()).readValue(config().toString(), HealthCheckConfig.class);

        healthCheck = new HealthCheck(this.vertx);

        startHttpServer().setHandler(ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
            }
            else {
                startFuture.fail(ar.cause());
            }
        });
    }

    private Future<HttpServer> startHttpServer() {
        Future<HttpServer> webServerFuture = Future.future();
        Router router = configureRouter();

        int port = config.getPort();

        LOG.info("Starting HealthCheck web server on port {}", port);
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
        router.get(REST_HEALTHZ).handler(healthCheckHandler);
        router.get(REST_READINESS).handler(readinessHandler);

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
        LOG.info("Shutting down webserver providing HealthCheck");
        server.close();
    }
}
