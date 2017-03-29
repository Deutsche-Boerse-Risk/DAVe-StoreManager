package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ProxyHelper;

import javax.inject.Inject;


public class PersistenceVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceVerticle.class);
    private final PersistenceService persistenceService;
    private PersistenceService proxyPersistenceService;

    @Inject
    public PersistenceVerticle(PersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    @Override
    public void start(Future<Void> fut) throws Exception {
        LOG.info("Starting {} with configuration: {}", PersistenceVerticle.class.getSimpleName(), config().encodePrettily());

        ProxyHelper.registerService(PersistenceService.class, vertx, this.persistenceService, PersistenceService.SERVICE_ADDRESS);
        this.proxyPersistenceService = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);

        this.proxyPersistenceService.initialize(ar -> {
            if (ar.succeeded()) {
                LOG.info("Persistence verticle started");
                fut.complete();
            } else {
                LOG.error("Persistence verticle failed to deploy", ar.cause());
                fut.fail(ar.cause());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        this.proxyPersistenceService.close();
        super.stop();
    }

}
