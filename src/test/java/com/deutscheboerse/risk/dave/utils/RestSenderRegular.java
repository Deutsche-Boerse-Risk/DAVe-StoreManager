package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.BaseTest;
import com.deutscheboerse.risk.dave.HttpVerticle;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.*;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RestSenderRegular extends BaseTest  implements RestSender {
    private static final Logger LOG = LoggerFactory.getLogger(RestSenderRegular.class);

    private static final String STORE_ACCOUNT_MARGIN_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.ACCOUNT_MARGIN_REQUEST_PARAMETER);
    private static final String STORE_LIQUI_GROUP_MARGIN_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.LIQUI_GROUP_MARGIN_REQUEST_PARAMETER);
    private static final String STORE_LIQUI_GROUP_SPLIT_MARGIN_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.LIQUI_GROUP_SPLIT_MARGIN_REQUEST_PARAMETER);
    private static final String STORE_POOL_MARGIN_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.POOL_MARGIN_REQUEST_PARAMETER);
    private static final String STORE_POSITION_REPORT_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.POSITION_REPORT_REQUEST_PARAMETER);
    private static final String STORE_RISK_LIMIT_UTILIZATION_API = String.format("%s/store/%s", HttpVerticle.API_PREFIX, HttpVerticle.RISK_LIMIT_UTILIZATION_REQUEST_PARAMETER);

    private final Vertx vertx;
    final HttpClient httpClient;

    public RestSenderRegular(Vertx vertx) {
        this.vertx = vertx;
        HttpClientOptions httpClientOptions = new HttpClientOptions().setSsl(true).setVerifyHost(false).setPemTrustOptions(BaseTest.HTTP_SERVER_CERTIFICATE.trustOptions());
        this.httpClient = this.vertx.createHttpClient(httpClientOptions);
    }

    public void sendAllData(Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();
        futures.add(this.sendData(STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER));
        futures.add(this.sendData(STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER));
        futures.add(this.sendData(STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER));
        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void sendAccountMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupSplitMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPoolMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPositionReportData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendRiskLimitUtilizationData(Handler<AsyncResult<Void>> handler) {
        this.sendData(STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    private Handler<AsyncResult<Void>> getResponseHandler(Handler<AsyncResult<Void>> handler) {
        return ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        };
    }

    private Future<Void> sendData(String requestURI, String folderName) {
        Future<Void> resultFuture = Future.future();
        final Collection<Integer> ttsaveNumbers = IntStream.rangeClosed(1, 2)
                .boxed()
                .collect(Collectors.toList());
        vertx.executeBlocking(future -> {
            CountDownLatch countDownLatch = new CountDownLatch(ttsaveNumbers.size());
            ttsaveNumbers.forEach(ttsaveNo -> this.sendModels(requestURI, folderName, ttsaveNo, res -> {
                if (res.succeeded()) {
                    countDownLatch.countDown();
                }
            }));
            try {
                countDownLatch.await(30, TimeUnit.SECONDS);
                future.complete();
            } catch (InterruptedException e) {
                future.fail(e.getCause());
            }
        }, resultFuture);
        return resultFuture;
    }

    private void sendModels(String requestURI, String folderName, int ttsaveNo, Handler<AsyncResult<Void>> resultHandler) {
        CountDownLatch countDownLatch = new CountDownLatch(DataHelper.getJsonObjectCount(folderName, ttsaveNo));
        DataHelper.readTTSaveFile(folderName, ttsaveNo, model -> this.postModel(requestURI, model, ar -> {
            if (ar.succeeded()) {
                countDownLatch.countDown();
            }
        }));
        try {
            if (countDownLatch.await(30, TimeUnit.SECONDS)) {
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture("Unable to send all models"));
            }
        } catch (InterruptedException e) {
            resultHandler.handle(Future.failedFuture("Unable to send all models"));
        }
    }

    protected void postModel(String requestURI, JsonObject model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                BaseTest.HTTP_PORT,
                "localhost",
                requestURI,
                response -> {
                    if (HttpResponseStatus.CREATED.code() == response.statusCode()) {
                        response.bodyHandler(body -> resultHandler.handle(Future.succeededFuture()));
                    } else {
                        LOG.error("Post failed: {}", response.statusMessage());
                        resultHandler.handle(Future.failedFuture(response.statusMessage()));
                    }
                })
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .end(model.encode());
    }
}
