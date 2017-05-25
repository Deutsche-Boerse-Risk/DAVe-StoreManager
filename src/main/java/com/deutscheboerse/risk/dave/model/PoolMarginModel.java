package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.grpc.PoolMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class PoolMarginModel implements Model<PoolMargin> {

    private final PoolMargin grpc;

    public PoolMarginModel(PoolMargin grpc) {
        this.grpc = grpc;
    }

    public PoolMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(PoolMargin.class);
    }

    public static PoolMarginModel buildFromJson(JsonObject json) {
        return new PoolMarginModel(PoolMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setPool(json.getString("pool"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setClrRptCurrency(json.getString("clrRptCurrency"))
                .setRequiredMargin(json.getDouble("requiredMargin"))
                .setCashCollateralAmount(json.getDouble("cashCollateralAmount"))
                .setAdjustedSecurities(json.getDouble("adjustedSecurities"))
                .setAdjustedGuarantee(json.getDouble("adjustedGuarantee"))
                .setOverUnderInMarginCurr(json.getDouble("overUnderInMarginCurr"))
                .setOverUnderInClrRptCurr(json.getDouble("overUnderInClrRptCurr"))
                .setVariPremInMarginCurr(json.getDouble("variPremInMarginCurr"))
                .setAdjustedExchangeRate(json.getDouble("adjustedExchangeRate"))
                .setPoolOwner(json.getString("poolOwner"))
                .build());
    }

    @Override
    public PoolMargin toGrpc() {
        return this.grpc;
    }

    @Override
    public JsonObject getMongoQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", this.grpc.getClearer());
        queryParams.put("pool", this.grpc.getPool());
        queryParams.put("marginCurrency", this.grpc.getMarginCurrency());
        queryParams.put("clrRptCurrency", this.grpc.getClrRptCurrency());
        queryParams.put("poolOwner", this.grpc.getPoolOwner());
        return queryParams;
    }

    @Override
    public JsonObject getMongoStoreDocument() {
        JsonObject document = new JsonObject();
        JsonObject snapshotDocument = new JsonObject();
        snapshotDocument.put("snapshotID", this.grpc.getSnapshotId());
        snapshotDocument.put("businessDate", this.grpc.getBusinessDate());
        snapshotDocument.put("timestamp", this.grpc.getTimestamp());
        snapshotDocument.put("requiredMargin", this.grpc.getRequiredMargin());
        snapshotDocument.put("cashCollateralAmount", this.grpc.getCashCollateralAmount());
        snapshotDocument.put("adjustedSecurities", this.grpc.getAdjustedSecurities());
        snapshotDocument.put("adjustedGuarantee", this.grpc.getAdjustedGuarantee());
        snapshotDocument.put("overUnderInMarginCurr", this.grpc.getOverUnderInMarginCurr());
        snapshotDocument.put("overUnderInClrRptCurr", this.grpc.getOverUnderInClrRptCurr());
        snapshotDocument.put("variPremInMarginCurr", this.grpc.getVariPremInMarginCurr());
        snapshotDocument.put("adjustedExchangeRate", this.grpc.getAdjustedExchangeRate());
        document.put("$set", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "PoolMargin";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "pool",
                        "marginCurrency",
                        "clrRptCurrency",
                        "poolOwner"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "requiredMargin",
                        "cashCollateralAmount",
                        "adjustedSecurities",
                        "adjustedGuarantee",
                        "overUnderInMarginCurr",
                        "overUnderInClrRptCurr",
                        "variPremInMarginCurr",
                        "adjustedExchangeRate"
                ));
            }
        };
    }
}
