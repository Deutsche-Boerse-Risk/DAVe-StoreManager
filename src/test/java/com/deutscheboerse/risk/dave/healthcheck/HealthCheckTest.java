package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.healthcheck.HealthCheck.Component;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckTest {

    @Test
    public void testInitialization(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx);

        for (Component component: Component.values()) {
            context.assertFalse(healthCheck.isComponentReady(component), component.name() + " readiness should be initialized to false");
        }

        context.assertFalse(healthCheck.ready(), "Initial state of healthCheck should be false");

        vertx.close();
    }

    @Test
    public void testNothingIsReady(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx);

        context.assertFalse(healthCheck.ready(), "Nothing is ready, should return false");

        vertx.close();
    }

    @Test
    public void testSomeVerticlesAreReady(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx)
                .setComponentReady(Component.HTTP);
        context.assertFalse(healthCheck.ready(), "Only some verticles are ready, should return false");

        vertx.close();
    }

    @Test
    public void testAllVerticlesAreReady(TestContext context) {
        Vertx vertx = Vertx.vertx();

        HealthCheck healthCheck = new HealthCheck(vertx)
                .setComponentReady(Component.HTTP)
                .setComponentReady(Component.PERSISTENCE_SERVICE);

        context.assertTrue(healthCheck.ready(), "All verticles are ready, the whole app should be ready");

        vertx.close();
    }
}
