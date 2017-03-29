package com.deutscheboerse.risk.dave.healthcheck;

import com.deutscheboerse.risk.dave.BaseTest;
import com.deutscheboerse.risk.dave.HttpVerticle;
import com.deutscheboerse.risk.dave.MainVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class HealthCheckIT extends BaseTest {

    private static Vertx vertx;

    @BeforeClass
    public static void setUp(TestContext context) {
        vertx = Vertx.vertx();

        DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(BaseTest.getGlobalConfig());
        vertx.deployVerticle(MainVerticle.class.getName(), deploymentOptions, context.asyncAssertSuccess());
    }

    private Handler<HttpClientResponse> assertEqualsHttpHandler(int expectedCode, String expectedText, TestContext context) {
        final Async async = context.async();
        return response -> {
            context.assertEquals(expectedCode, response.statusCode());
            response.bodyHandler(body -> {
                try {
                    context.assertEquals(expectedText, body.toString());
                    async.complete();
                } catch (Exception e) {
                    context.fail(e);
                }
            });
        };
    }

    @Test
    public void testHealth(TestContext context) throws InterruptedException {
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "healthz")
                .put("status", "UP")))
                .put("outcome", "UP");
        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HttpVerticle.REST_HEALTHZ,
                assertEqualsHttpHandler(200, expected.encode(), context));
    }

    @Test
    public void testReadinessOk(TestContext context) throws InterruptedException {
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "readiness")
                .put("status", "UP")))
                .put("outcome", "UP");
        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HttpVerticle.REST_READINESS,
                assertEqualsHttpHandler(200, expected.encode(), context));
    }

    @Test
    public void testReadinessNok(TestContext context) throws InterruptedException {
        JsonObject expected = new JsonObject();
        expected.put("checks", new JsonArray().add(new JsonObject()
                .put("id", "readiness")
                .put("status", "DOWN")))
                .put("outcome", "DOWN");

        HealthCheck healthCheck = new HealthCheck(vertx);
        healthCheck.setComponentFailed(HealthCheck.Component.PERSISTENCE_SERVICE);

        vertx.createHttpClient().getNow(HTTP_PORT, "localhost", HttpVerticle.REST_READINESS,
                assertEqualsHttpHandler(503, expected.encode(), context));
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }
}
