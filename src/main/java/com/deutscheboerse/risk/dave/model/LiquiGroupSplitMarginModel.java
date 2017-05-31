package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.grpc.LiquiGroupSplitMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class LiquiGroupSplitMarginModel implements Model<LiquiGroupSplitMargin> {

    private final LiquiGroupSplitMargin grpc;

    public LiquiGroupSplitMarginModel(LiquiGroupSplitMargin grpc) {
        this.grpc = grpc;
    }

    public LiquiGroupSplitMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(LiquiGroupSplitMargin.class);
    }

    public static LiquiGroupSplitMarginModel buildFromJson(JsonObject json) {
        return new LiquiGroupSplitMarginModel(LiquiGroupSplitMargin.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setLiquidationGroup(json.getString("liquidationGroup"))
                .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                .setMarginCurrency(json.getString("marginCurrency"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setMarketRisk(json.getDouble("marketRisk"))
                .setLiquRisk(json.getDouble("liquRisk"))
                .setLongOptionCredit(json.getDouble("longOptionCredit"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .build());
    }

    @Override
    public LiquiGroupSplitMargin toGrpc() {
        return this.grpc;
    }

    @Override
    public JsonObject getMongoQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", this.grpc.getClearer());
        queryParams.put("member", this.grpc.getMember());
        queryParams.put("account", this.grpc.getAccount());
        queryParams.put("liquidationGroup", this.grpc.getLiquidationGroup());
        queryParams.put("liquidationGroupSplit", this.grpc.getLiquidationGroupSplit());
        queryParams.put("marginCurrency", this.grpc.getMarginCurrency());
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
        snapshotDocument.put("marketRisk", this.grpc.getMarketRisk());
        snapshotDocument.put("liquRisk", this.grpc.getLiquRisk());
        snapshotDocument.put("longOptionCredit", this.grpc.getLongOptionCredit());
        snapshotDocument.put("variationPremiumPayment", this.grpc.getVariationPremiumPayment());
        document.put("$setOnInsert", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "LiquiGroupSplitMargin";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "member",
                        "account",
                        "liquidationGroup",
                        "liquidationGroupSplit",
                        "marginCurrency"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "premiumMargin",
                        "marketRisk",
                        "liquRisk",
                        "longOptionCredit",
                        "variationPremiumPayment"
                ));
            }
        };
    }
}
