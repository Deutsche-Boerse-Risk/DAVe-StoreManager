package com.deutscheboerse.risk.dave.utils;


import io.vertx.core.json.JsonObject;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static java.util.stream.Collectors.joining;

public class URIBuilder {
    private final String basePath;
    private final JsonObject params = new JsonObject();

    public URIBuilder(String basePath) {
        this.basePath = basePath;
    }

    public URIBuilder addParams(JsonObject params) {
        this.params.mergeIn(params);
        return this;
    }

    public String build() {
        String queryString = this.params.stream()
                .map(e -> String.format("%s=%s", e.getKey(), urlEncode(e.getValue().toString())))
                .collect(joining("&"));

        return queryString.isEmpty() ? basePath : basePath + "?" + queryString;
    }

    private String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("Unsupported encoding", e);
        }
    }
}
