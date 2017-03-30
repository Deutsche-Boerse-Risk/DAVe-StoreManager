package com.deutscheboerse.risk.dave;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MainVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);
    private static final String MONGO_CONF_KEY = "mongo";
    private static final String HTTP_CONF_KEY = "http";
    private static final int HTTP_VERTICLE_INSTANCES = 5;
    private JsonObject configuration;
    private Map<String, String> verticleDeployments = new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) {
        Future<Void> chainFuture = Future.future();
        this.retrieveConfig()
                .compose(i -> deployPersistenceVerticle())
                .compose(i -> deployHttpVerticle())
                .compose(chainFuture::complete, chainFuture);

        chainFuture.setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("All verticles deployed");
                startFuture.complete();
            } else {
                LOG.error("Fail to deploy some verticle");
                closeAllDeployments();
                startFuture.fail(chainFuture.cause());
            }
        });
    }

    private void addHoconConfigStoreOptions(ConfigRetrieverOptions options) {
        String configurationFile = System.getProperty("dave.configurationFile");
        if (configurationFile != null) {
            options.addStore(new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("hocon")
                    .setConfig(new JsonObject()
                            .put("path", configurationFile)));
        }
    }

    private void addDeploymentConfigStoreOptions(ConfigRetrieverOptions options) {
        options.addStore(new ConfigStoreOptions().setType("json").setConfig(vertx.getOrCreateContext().config()));
    }

    private Future<Void> retrieveConfig() {
        Future<Void> future = Future.future();
        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        this.addHoconConfigStoreOptions(options);
        this.addDeploymentConfigStoreOptions(options);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                this.configuration = ar.result();
                LOG.debug("Retrieved configuration: {}", this.configuration.encodePrettily());
                future.complete();
            } else {
                LOG.error("Unable to retrieve configuration", ar.cause());
                future.fail(ar.cause());
            }
        });
        return future;
    }

    private Future<Void> deployPersistenceVerticle() {
        return this.deployVerticle(PersistenceVerticle.class, this.configuration.getJsonObject(MONGO_CONF_KEY,
                new JsonObject()).put("guice_binder", this.configuration.getString("guice_binder", PersistenceVerticleBinder.class.getName())), 1);
    }

    private Future<Void> deployHttpVerticle() {
        return this.deployVerticle(HttpVerticle.class, this.configuration.getJsonObject(HTTP_CONF_KEY, new JsonObject()), HTTP_VERTICLE_INSTANCES);
    }

    private Future<Void> deployVerticle(Class clazz, JsonObject config, int instances) {
        Future<Void> verticleFuture = Future.future();
        DeploymentOptions options = new DeploymentOptions().setConfig(config).setInstances(instances);
        String deploymentName = String.format("%s%s", config.containsKey("guice_binder") ? "java-guice:" : "", clazz.getName());
        vertx.deployVerticle(deploymentName, options, ar -> {
            if (ar.succeeded()) {
                LOG.info("Deployed {} with ID {}", clazz.getName(), ar.result());
                verticleDeployments.put(clazz.getSimpleName(), ar.result());
                verticleFuture.complete();
            } else {
                if (!verticleFuture.failed()) {
                    verticleFuture.fail(ar.cause());
                }
            }
        });
        return verticleFuture;
    }

    private void closeAllDeployments() {
        LOG.info("Undeploying verticles");

        List<Future> futures = new LinkedList<>();
        this.verticleDeployments.forEach((verticleName, deploymentID) -> {
            if (deploymentID != null && vertx.deploymentIDs().contains(deploymentID)) {
                LOG.info("Undeploying {} with ID: {}", verticleName, deploymentID);
                Future<Void> future = Future.future();
                vertx.undeploy(deploymentID, future.completer());
                futures.add(future);
            }
        });

        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                LOG.info("Undeployed all verticles");
            } else {
                LOG.error("Failed to undeploy some verticles", ar.cause());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping main verticle");
        this.closeAllDeployments();
        super.stop();
    }
}
