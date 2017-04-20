package com.deutscheboerse.risk.dave.config;

public class MongoConfig {
    private String dbName = "DAVe";
    private String connectionUrl = "mongodb://localhost:27017/?waitqueuemultiple=20000";
    private String guice_binder = null;

    public String getDbName() {
        return dbName;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getGuice_binder() {
        return guice_binder;
    }
}
