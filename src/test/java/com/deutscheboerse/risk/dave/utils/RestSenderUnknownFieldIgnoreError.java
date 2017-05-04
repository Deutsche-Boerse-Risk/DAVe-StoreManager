package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RestSenderUnknownFieldIgnoreError extends RestSenderRegular {
    public RestSenderUnknownFieldIgnoreError(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected void postModels(String requestURI, JsonArray models, Handler<AsyncResult<Void>> resultHandler) {
        models.forEach(json -> ((JsonObject) json).put("unknown", "value"));
        this.httpClient.request(HttpMethod.POST,
                TestConfig.API_PORT,
                "localhost",
                requestURI,
                response -> response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture())))
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(models.encode());
    }
}
