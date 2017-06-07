package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.grpc.AccountMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class AccountMarginModel implements Model<AccountMargin> {

    private final AccountMargin grpc;

    public AccountMarginModel(AccountMargin grpc) {
        this.grpc = grpc;
    }

    public AccountMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(AccountMargin.class);
    }

    public static AccountMarginModel buildFromJson(JsonObject json) {
        return new AccountMarginModel(AccountMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setPool(json.getString("pool"))
                .setMarginReqInMarginCurr(json.getDouble("marginReqInMarginCurr"))
                .setMarginReqInClrCurr(json.getDouble("marginReqInClrCurr"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build());
    }

    @Override
    public AccountMargin toGrpc() {
        return this.grpc;
    }

    @Override
    public JsonObject getMongoQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", this.grpc.getClearer());
        queryParams.put("member", this.grpc.getMember());
        queryParams.put("account", this.grpc.getAccount());
        queryParams.put("marginCurrency", this.grpc.getMarginCurrency());
        queryParams.put("clearingCurrency", this.grpc.getClearingCurrency());
        queryParams.put("pool", this.grpc.getPool());
        return queryParams;
    }

    @Override
    public JsonObject getMongoStoreDocument() {
        JsonObject document = new JsonObject();
        JsonObject snapshotDocument = new JsonObject();
        snapshotDocument.put("snapshotID", this.grpc.getSnapshotId());
        snapshotDocument.put("businessDate", this.grpc.getBusinessDate());
        snapshotDocument.put("timestamp", this.grpc.getTimestamp());
        snapshotDocument.put("marginReqInMarginCurr", this.grpc.getMarginReqInMarginCurr());
        snapshotDocument.put("marginReqInClrCurr", this.grpc.getMarginReqInClrCurr());
        snapshotDocument.put("unadjustedMarginRequirement", this.grpc.getUnadjustedMarginRequirement());
        snapshotDocument.put("variationPremiumPayment", this.grpc.getVariationPremiumPayment());
        document.put("$setOnInsert", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "AccountMargin";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "member",
                        "account",
                        "marginCurrency",
                        "clearingCurrency",
                        "pool"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "marginReqInMarginCurr",
                        "marginReqInClrCurr",
                        "unadjustedMarginRequirement",
                        "variationPremiumPayment"
                ));
            }
        };
    }
}
