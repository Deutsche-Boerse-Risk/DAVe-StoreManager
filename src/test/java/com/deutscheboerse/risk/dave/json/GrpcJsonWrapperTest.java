package com.deutscheboerse.risk.dave.json;

import com.deutscheboerse.risk.dave.grpc.AccountMargin;
import io.grpc.Status;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;

@RunWith(VertxUnitRunner.class)
public class GrpcJsonWrapperTest {

    private static GrpcJsonWrapper wrapper;

    @BeforeClass
    public static void setUp() {
        wrapper = new GrpcJsonWrapper(AccountMargin.newBuilder().setClearer("ABCDE").build());
    }

    @Test
    public void testMapTo(TestContext context) {
        AccountMargin accountMargin = AccountMargin.newBuilder().setClearer("ABCDE").build();
        context.assertEquals(wrapper.mapTo(AccountMargin.class), accountMargin);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetString() {
        wrapper.getString("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInteger() {
        wrapper.getInteger("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong() {
        wrapper.getLong("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDouble() {
        wrapper.getDouble("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloat() {
        wrapper.getFloat("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBoolean() {
        wrapper.getBoolean("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetJsonObject() {
        wrapper.getJsonObject("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetJsonArray() {
        wrapper.getJsonArray("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinary() {
        wrapper.getBinary("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInstant() {
        wrapper.getInstant("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetValue() {
        wrapper.getValue("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStringDefault() {
        wrapper.getString("key", "");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIntegerDefault() {
        wrapper.getInteger("key", 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLongDefault() {
        wrapper.getLong("key", 0L);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDoubleDefault() {
        wrapper.getDouble("key", 0.0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloatDefault() {
        wrapper.getFloat("key", 0.0f);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBooleanDefault() {
        wrapper.getBoolean("key", false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetJsonObjectDefault() {
        wrapper.getJsonObject("key", new JsonObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetJsonArrayDefault() {
        wrapper.getJsonArray("key", new JsonArray());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinaryDefault() {
        wrapper.getBinary("key", new byte[]{});
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInstantDefault() {
        wrapper.getInstant("key", Instant.now());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetValueDefault() {
        wrapper.getValue("key", new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContainsKey() {
        wrapper.containsKey("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFieldNames() {
        wrapper.fieldNames();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutEnum() {
        wrapper.put("key", Status.Code.OK);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutCharSequence() {
        wrapper.put("key", (CharSequence)"");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutString() {
        wrapper.put("key", "");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutInteger() {
        wrapper.put("key", 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutLong() {
        wrapper.put("key", 0L);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutDouble() {
        wrapper.put("key", 0.0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutFloat() {
        wrapper.put("key", 0.0f);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutBoolean() {
        wrapper.put("key", false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutNull() {
        wrapper.putNull("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutJsonObject() {
        wrapper.put("key", new JsonObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutJsonArray() {
        wrapper.put("key", new JsonArray());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutByteArray() {
        wrapper.put("key", new byte[]{});
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutInstant() {
        wrapper.put("key", Instant.now());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutObject() {
        wrapper.put("key", new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        wrapper.remove("key");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeIn() {
        wrapper.mergeIn(new JsonObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeInDeep() {
        wrapper.mergeIn(new JsonObject(), false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeInDepth() {
        wrapper.mergeIn(new JsonObject(), 0);
    }

    @Test
    public void testEncode(TestContext context) {
        context.assertEquals(wrapper.encode(), "clearer: \"ABCDE\"\n");
    }

    @Test
    public void testEncodePrettily(TestContext context) {
        context.assertEquals(wrapper.encodePrettily(), "clearer: \"ABCDE\"\n");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToBuffer() {
        wrapper.toBuffer();
    }

    @Test
    public void testCopy(TestContext context) {
        context.assertEquals(wrapper, wrapper.copy());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMap() {
        wrapper.getMap();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStream() {
        wrapper.stream();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator() {
        wrapper.iterator();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSize() {
        wrapper.size();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear() {
        wrapper.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsEmpty() {
        wrapper.isEmpty();
    }

    @Test
    public void testToString(TestContext context) {
        context.assertEquals(wrapper.toString(), "clearer: \"ABCDE\"\n");
    }

    @Test
    public void testEquals(TestContext context) {
        GrpcJsonWrapper localWrapper = new GrpcJsonWrapper(AccountMargin.newBuilder().setClearer("ABCDE").build());
        context.assertEquals(wrapper, localWrapper);
        context.assertNotEquals(wrapper, null);
    }

    @Test
    public void testHashCode(TestContext context) {
        GrpcJsonWrapper localWrapper = new GrpcJsonWrapper(AccountMargin.newBuilder().setClearer("ABCDE").build());
        context.assertEquals(wrapper.hashCode(), localWrapper.hashCode());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteToBuffer() {
        Buffer buffer = Buffer.buffer();
        wrapper.writeToBuffer(buffer);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadFromBuffer() {
        Buffer buffer = Buffer.buffer();
        wrapper.readFromBuffer(0, buffer);
    }
}
