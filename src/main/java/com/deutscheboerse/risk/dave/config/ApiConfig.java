package com.deutscheboerse.risk.dave.config;

public class ApiConfig {
    private int port = 8443;
    private String sslKey = null;
    private String sslCert = null;
    private String[] sslTrustCerts = new String[] {};
    private boolean sslRequireClientAuth = false;

    public int getPort() {
        return port;
    }

    public String getSslKey() {
        return sslKey;
    }

    public String getSslCert() {
        return sslCert;
    }

    public String[] getSslTrustCerts() {
        return sslTrustCerts;
    }

    public boolean isSslRequireClientAuth() {
        return sslRequireClientAuth;
    }
}
