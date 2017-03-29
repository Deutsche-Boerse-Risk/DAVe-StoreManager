package com.deutscheboerse.risk.dave;

import com.deutscheboerse.risk.dave.persistence.InitPersistenceService;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import com.google.inject.AbstractModule;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class PersistenceVerticleIT {
    private Vertx vertx;
    private static InitPersistenceService persistenceService;

    @Test
    public void checkPersistenceServiceInitialized(TestContext context) {
        this.vertx = Vertx.vertx();
        PersistenceVerticleIT.persistenceService = new InitPersistenceService(true);
        JsonObject config = new JsonObject().put("guice_binder", TestBinder.class.getName());
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Async async = context.async();
        this.vertx.deployVerticle("java-guice:" + PersistenceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                async.complete();
            } else {
                context.fail((ar.cause()));
            }
        });
        async.awaitSuccess(10000);
        context.assertTrue(persistenceService.isInitialized());
    }

    @Test
    public void checkPersistenceServiceNotInitialized(TestContext context) {
        this.vertx = Vertx.vertx();
        PersistenceVerticleIT.persistenceService = new InitPersistenceService(false);
        JsonObject config = new JsonObject().put("guice_binder", TestBinder.class.getName());
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Async async = context.async();
        this.vertx.deployVerticle("java-guice:" + PersistenceVerticle.class.getName(), options, ar -> {
            if (ar.succeeded()) {
                context.fail((ar.cause()));
            } else {
                async.complete();
            }
        });
        async.awaitSuccess(10000);
        context.assertFalse(persistenceService.isInitialized());
    }

    @After
    public void tearDown(TestContext context) {
        this.vertx.close(context.asyncAssertSuccess());
    }

    public static class TestBinder extends AbstractModule {

        @Override
        protected void configure() {
            bind(PersistenceService.class).toInstance(persistenceService);
        }
    }
}
