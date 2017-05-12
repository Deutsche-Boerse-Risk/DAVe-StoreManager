package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.PositionReport;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class PositionReportModel implements Model<PositionReport> {

    private final PositionReport grpc;

    public PositionReportModel(PositionReport grpc) {
        this.grpc = grpc;
    }

    public PositionReportModel(JsonObject json) {
        verifyJson(json);
        try {
            this.grpc = PositionReport.parseFrom(json.getBinary("grpc"));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static PositionReportModel buildFromJson(JsonObject json) {
        return new PositionReportModel(PositionReport.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setAccount(json.getString("account"))
                .setLiquidationGroup(json.getString("liquidationGroup"))
                .setLiquidationGroupSplit(json.getString("liquidationGroupSplit"))
                .setProduct(json.getString("product"))
                .setCallPut(json.getString("callPut"))
                .setContractYear(json.getInteger("contractYear"))
                .setContractMonth(json.getInteger("contractMonth"))
                .setExpiryDay(json.getInteger("expiryDay"))
                .setExercisePrice(json.getDouble("exercisePrice"))
                .setVersion(json.getString("version"))
                .setFlexContractSymbol(json.getString("flexContractSymbol"))
                .setNetQuantityLs(json.getDouble("netQuantityLs"))
                .setNetQuantityEa(json.getDouble("netQuantityEa"))
                .setClearingCurrency(json.getString("clearingCurrency"))
                .setMVar(json.getDouble("mVar"))
                .setCompVar(json.getDouble("compVar"))
                .setCompCorrelationBreak(json.getDouble("compCorrelationBreak"))
                .setCompCompressionError(json.getDouble("compCompressionError"))
                .setCompLiquidityAddOn(json.getDouble("compLiquidityAddOn"))
                .setCompLongOptionCredit(json.getDouble("compLongOptionCredit"))
                .setProductCurrency(json.getString("productCurrency"))
                .setVariationPremiumPayment(json.getDouble("variationPremiumPayment"))
                .setPremiumMargin(json.getDouble("premiumMargin"))
                .setNormalizedDelta(json.getDouble("normalizedDelta"))
                .setNormalizedGamma(json.getDouble("normalizedGamma"))
                .setNormalizedVega(json.getDouble("normalizedVega"))
                .setNormalizedRho(json.getDouble("normalizedRho"))
                .setNormalizedTheta(json.getDouble("normalizedTheta"))
                .setUnderlying(json.getString("underlying"))
                .build());
    }

    @Override
    public PositionReport toGrpc() {
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
        queryParams.put("product", this.grpc.getProduct());
        queryParams.put("callPut", this.grpc.getCallPut());
        queryParams.put("contractYear", this.grpc.getContractYear());
        queryParams.put("contractMonth", this.grpc.getContractMonth());
        queryParams.put("expiryDay", this.grpc.getExpiryDay());
        queryParams.put("exercisePrice", this.grpc.getExercisePrice());
        queryParams.put("version", this.grpc.getVersion());
        queryParams.put("flexContractSymbol", this.grpc.getFlexContractSymbol());
        queryParams.put("clearingCurrency", this.grpc.getClearingCurrency());
        queryParams.put("productCurrency", this.grpc.getProductCurrency());
        queryParams.put("underlying", this.grpc.getUnderlying());
        return queryParams;
    }

    @Override
    public JsonObject getMongoStoreDocument() {
        JsonObject document = new JsonObject();
        JsonObject snapshotDocument = new JsonObject();
        snapshotDocument.put("snapshotID", this.grpc.getSnapshotId());
        snapshotDocument.put("businessDate", this.grpc.getBusinessDate());
        snapshotDocument.put("timestamp", this.grpc.getTimestamp());
        snapshotDocument.put("netQuantityLs", this.grpc.getNetQuantityLs());
        snapshotDocument.put("netQuantityEa", this.grpc.getNetQuantityEa());
        snapshotDocument.put("mVar", this.grpc.getMVar());
        snapshotDocument.put("compVar", this.grpc.getCompVar());
        snapshotDocument.put("compCorrelationBreak", this.grpc.getCompCorrelationBreak());
        snapshotDocument.put("compCompressionError", this.grpc.getCompCompressionError());
        snapshotDocument.put("compLiquidityAddOn", this.grpc.getCompLiquidityAddOn());
        snapshotDocument.put("compLongOptionCredit", this.grpc.getCompLongOptionCredit());
        snapshotDocument.put("variationPremiumPayment", this.grpc.getVariationPremiumPayment());
        snapshotDocument.put("premiumMargin", this.grpc.getPremiumMargin());
        snapshotDocument.put("normalizedDelta", this.grpc.getNormalizedDelta());
        snapshotDocument.put("normalizedGamma", this.grpc.getNormalizedGamma());
        snapshotDocument.put("normalizedVega", this.grpc.getNormalizedVega());
        snapshotDocument.put("normalizedRho", this.grpc.getNormalizedRho());
        snapshotDocument.put("normalizedTheta", this.grpc.getNormalizedTheta());
        document.put("$set", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "PositionReport";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "member",
                        "account",
                        "liquidationGroup",
                        "liquidationGroupSplit",
                        "product",
                        "callPut",
                        "contractYear",
                        "contractMonth",
                        "expiryDay",
                        "exercisePrice",
                        "version",
                        "flexContractSymbol",
                        "clearingCurrency",
                        "productCurrency",
                        "underlying"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "netQuantityLs",
                        "netQuantityEa",
                        "mVar",
                        "compVar",
                        "compCorrelationBreak",
                        "compCompressionError",
                        "compLiquidityAddOn",
                        "compLongOptionCredit",
                        "variationPremiumPayment",
                        "premiumMargin",
                        "normalizedDelta",
                        "normalizedGamma",
                        "normalizedVega",
                        "normalizedRho",
                        "normalizedTheta"
                ));
            }
        };
    }
}
