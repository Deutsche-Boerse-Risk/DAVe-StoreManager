package com.deutscheboerse.risk.dave.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

public class RestSenderRegularIgnoreError extends RestSenderRegular {
    public RestSenderRegularIgnoreError(Vertx vertx) {
        super(vertx);
    }

    protected void postModel(String requestURI, JsonObject model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                TestConfig.API_PORT,
                "localhost",
                requestURI,
                response -> response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture())))
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(model.encode());
    }
}
