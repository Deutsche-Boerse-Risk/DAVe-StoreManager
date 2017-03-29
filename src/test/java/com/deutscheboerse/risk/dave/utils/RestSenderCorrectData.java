package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.HttpVerticle;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.*;
import io.vertx.core.http.HttpClient;
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

public class RestSenderCorrectData implements RestSender {
    private static final Logger LOG = LoggerFactory.getLogger(RestSenderCorrectData.class);

    private final Vertx vertx;
    private final HttpClient httpClient;
    private final int tcpPort;

    public RestSenderCorrectData(Vertx vertx) {
        this.vertx = vertx;
        this.httpClient = this.vertx.createHttpClient();
        this.tcpPort = Integer.getInteger("http.port", 8083);
    }

    public void sendAllData(Handler<AsyncResult<Void>> handler) {
        List<Future> futures = new ArrayList<>();
        futures.add(this.sendData(HttpVerticle.STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER));
        futures.add(this.sendData(HttpVerticle.STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER));
        futures.add(this.sendData(HttpVerticle.STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER));
        futures.add(this.sendData(HttpVerticle.STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER));
        futures.add(this.sendData(HttpVerticle.STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER));
        futures.add(this.sendData(HttpVerticle.STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER));
        CompositeFuture.all(futures).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });

/*
        this.sendData(HttpVerticle.STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER)
                .compose(i -> this.sendData(HttpVerticle.STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER))
                .compose(i -> this.sendData(HttpVerticle.STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER))
                .compose(i -> this.sendData(HttpVerticle.STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER))
                .compose(i -> this.sendData(HttpVerticle.STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER))
                .compose(i -> this.sendData(HttpVerticle.STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER))
                .setHandler(this.getResponseHandler(handler));
*/
    }

    public void sendAccountMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_ACCOUNT_MARGIN_API, DataHelper.ACCOUNT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_LIQUI_GROUP_MARGIN_API, DataHelper.LIQUI_GROUP_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendLiquiGroupSplitMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_LIQUI_GROUP_SPLIT_MARGIN_API, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPoolMarginData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_POOL_MARGIN_API, DataHelper.POOL_MARGIN_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendPositionReportData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_POSITION_REPORT_API, DataHelper.POSITION_REPORT_FOLDER).setHandler(this.getResponseHandler(handler));
    }

    public void sendRiskLimitUtilizationData(Handler<AsyncResult<Void>> handler) {
        this.sendData(HttpVerticle.STORE_RISK_LIMIT_UTILIZATION_API, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER).setHandler(this.getResponseHandler(handler));
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
        CountDownLatch countDownLatch = new CountDownLatch(ttsaveNumbers.size());
        ttsaveNumbers.forEach(ttsaveNo -> {
            this.sendModels(requestURI, folderName, ttsaveNo, res -> {
                if (res.succeeded()) {
                    countDownLatch.countDown();
                    if (countDownLatch.getCount() == 0) {
                        resultFuture.complete();
                    }
                }
            });
        });
        return resultFuture;
/*
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Unable to send all models", e);
        }
        if (countDownLatch.getCount() == 0) {
            LOG.info("All snapshots from folder {} sent", folderName);
            return Future.succeededFuture();
        } else {
            return Future.failedFuture("Unable to send all models");
        }
*/
    }

    private void sendModels(String requestURI, String folderName, int ttsaveNo, Handler<AsyncResult<Void>> resultHandler) {
        CountDownLatch countDownLatch = new CountDownLatch(DataHelper.getJsonObjectCount(folderName, ttsaveNo));
        DataHelper.readTTSaveFile(folderName, ttsaveNo, model -> {
            this.postModel(requestURI, model, ar -> {
                if (ar.succeeded()) {
                    countDownLatch.countDown();
                }
            });
        });
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Unable to send all models", e);
        }
        if (countDownLatch.getCount() == 0) {
            resultHandler.handle(Future.succeededFuture());
        } else {
            resultHandler.handle(Future.failedFuture("Unable to send all models"));
        }
    }

    private void postModel(String requestURI, JsonObject model, Handler<AsyncResult<Void>> resultHandler) {
        this.httpClient.request(HttpMethod.POST,
                this.tcpPort,
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
