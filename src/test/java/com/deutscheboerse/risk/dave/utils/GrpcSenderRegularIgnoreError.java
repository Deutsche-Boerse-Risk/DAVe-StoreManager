package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.grpc.StoreReply;
import com.deutscheboerse.risk.dave.model.Model;
import com.google.protobuf.MessageLite;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.GrpcUniExchange;

import java.util.function.Consumer;
import java.util.function.Function;

public class GrpcSenderRegularIgnoreError extends GrpcSenderRegular {
    public GrpcSenderRegularIgnoreError(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected <U extends MessageLite> void sendModels(Consumer<Handler<GrpcUniExchange<U, StoreReply>>> storeFunction, Function<JsonObject, Model<U>> grpcFunction, String folderName, int ttsaveNo, Handler<AsyncResult<Void>> resultHandler) {
        storeFunction.accept(exchange -> { exchange
                .handler(ar -> resultHandler.handle(Future.succeededFuture()));
            DataHelper.readTTSaveFile(folderName, ttsaveNo).forEach(json -> {
                U grpcModel = grpcFunction.apply(json).toGrpc();
                exchange.write(grpcModel);
            });
            exchange.end();
        });
    }
}
