package io.vertx.ext.mongo.impl.config;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.config.MongoBulkClientOptionsParser;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(VertxUnitRunner.class)
public class MongoBulkClientOptionsParserTest {
    @Test
    public void testConnStringDbName(TestContext context) {
        String connectionString = "mongodb://localhost:27017/mydb";
        JsonObject config = new JsonObject().put("connection_string", connectionString).put("db_name", "unused_db");

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        context.assertEquals("mydb", parser.database());
    }

    @Test
    public void testDbName(TestContext context) {
        String connectionString = "mongodb://localhost:27017/";
        JsonObject config = new JsonObject().put("connection_string", connectionString).put("db_name", "my_db");

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        context.assertEquals("my_db", parser.database());
    }

    @Test
    public void testWriteConcern(TestContext context) {
        String connectionString = "mongodb://localhost:27017/?journal=true";
        JsonObject config = new JsonObject().put("connection_string", connectionString).put("db_name", "my_db");

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        context.assertTrue(parser.settings().getWriteConcern().getJournal());
    }

    @Test
    public void testReadPreference(TestContext context) {
        String connectionString = "mongodb://localhost:27017/?readpreference=primary";
        JsonObject config = new JsonObject().put("connection_string", connectionString).put("db_name", "my_db");

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        context.assertEquals("primary", parser.settings().getReadPreference().getName());
    }

    @Test
    public void testHeartbeatSocket(TestContext context) {
        String connectionString = "mongodb://localhost:27017/?readpreference=primary";
        JsonObject config = new JsonObject().put("connection_string", connectionString)
                .put("db_name", "my_db")
                .put("heartbeat.socket", new JsonObject().put("socketTimeoutMS", 1000));

        MongoBulkClientOptionsParser parser = new MongoBulkClientOptionsParser(config);
        context.assertEquals(1000, parser.settings().getHeartbeatSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS));
    }
}
