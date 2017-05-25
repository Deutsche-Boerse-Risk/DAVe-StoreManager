package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.grpc.LiquiGroupMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class LiquiGroupMarginModel implements Model<LiquiGroupMargin> {

    private final LiquiGroupMargin grpc;

    public LiquiGroupMarginModel(LiquiGroupMargin grpc) {
        this.grpc = grpc;
    }

    public LiquiGroupMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(LiquiGroupMargin.class);
    }

    public static LiquiGroupMarginModel buildFromJson(JsonObject json) {
        return new LiquiGroupMarginModel(LiquiGroupMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setMarginClass(json.getString("marginClass"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setMarginGroup(json.getString("marginGroup"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setCurrentLiquidatingMargin(json.getDouble("currentLiquidatingMargin"))
                .setFuturesSpreadMargin(json.getDouble("futuresSpreadMargin"))
                .setAdditionalMargin(json.getDouble("additionalMargin"))
                .setUnadjustedMarginRequirement(json.getDouble("unadjustedMarginRequirement"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build());
    }

    @Override
    public LiquiGroupMargin toGrpc() {
        return this.grpc;
    }

    @Override
    public JsonObject getMongoQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", this.grpc.getClearer());
        queryParams.put("member", this.grpc.getMember());
        queryParams.put("account", this.grpc.getAccount());
        queryParams.put("marginClass", this.grpc.getMarginClass());
        queryParams.put("marginCurrency", this.grpc.getMarginCurrency());
        queryParams.put("marginGroup", this.grpc.getMarginGroup());
        return queryParams;
    }

    @Override
    public JsonObject getMongoStoreDocument() {
        JsonObject document = new JsonObject();
        JsonObject snapshotDocument = new JsonObject();
        snapshotDocument.put("snapshotID", this.grpc.getSnapshotId());
        snapshotDocument.put("businessDate", this.grpc.getBusinessDate());
        snapshotDocument.put("timestamp", this.grpc.getTimestamp());
        snapshotDocument.put("premiumMargin", this.grpc.getPremiumMargin());
        snapshotDocument.put("currentLiquidatingMargin", this.grpc.getCurrentLiquidatingMargin());
        snapshotDocument.put("futuresSpreadMargin", this.grpc.getFuturesSpreadMargin());
        snapshotDocument.put("additionalMargin", this.grpc.getAdditionalMargin());
        snapshotDocument.put("unadjustedMarginRequirement", this.grpc.getUnadjustedMarginRequirement());
        snapshotDocument.put("variationPremiumPayment", this.grpc.getVariationPremiumPayment());
        document.put("$set", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "LiquiGroupMargin";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "member",
                        "account",
                        "marginClass",
                        "marginCurrency",
                        "marginGroup"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "premiumMargin",
                        "currentLiquidatingMargin",
                        "futuresSpreadMargin",
                        "additionalMargin",
                        "unadjustedMarginRequirement",
                        "variationPremiumPayment"
                ));
            }
        };
    }
}
