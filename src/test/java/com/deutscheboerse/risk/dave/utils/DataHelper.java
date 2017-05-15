package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.*;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.RequestType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(DataHelper.class);

    public static final String ACCOUNT_MARGIN_FOLDER = "accountMargin";
    public static final String LIQUI_GROUP_MARGIN_FOLDER = "liquiGroupMargin";
    public static final String LIQUI_GROUP_SPLIT_MARGIN_FOLDER = "liquiGroupSplitMargin";
    public static final String POOL_MARGIN_FOLDER = "poolMargin";
    public static final String POSITION_REPORT_FOLDER = "positionReport";
    public static final String RISK_LIMIT_UTILIZATION_FOLDER = "riskLimitUtilization";

    private static Optional<JsonArray> getJsonArrayFromTTSaveFile(String folderName, int ttsaveNo) {
        String jsonPath = String.format("%s/snapshot-%03d.json", MainVerticleIT.class.getResource(folderName).getPath(), ttsaveNo);
        try {
            byte[] jsonArrayBytes = Files.readAllBytes(Paths.get(jsonPath));
            JsonArray jsonArray = new JsonArray(new String(jsonArrayBytes, Charset.defaultCharset()));
            return Optional.of(jsonArray);
        } catch (IOException e) {
            LOG.error("Unable to read data from {}", jsonPath, e);
            return Optional.empty();
        }
    }

    public static void readTTSaveFile(String folderName, int ttsaveNo, Consumer<JsonObject> consumer) {
        getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .forEach(json -> consumer.accept((JsonObject) json));
    }

    public static List<JsonObject> readTTSaveFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .collect(Collectors.toList());
    }

    public static Optional<JsonObject> getLastJsonFromFile(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .stream()
                .map(json -> (JsonObject) json)
                .reduce((a, b) -> b);
    }

    public static <T extends Model> T getLastModelFromFile(String folderName, int ttsaveNo,
                                                           Function<JsonObject, T> modelFactory) {
        return modelFactory.apply(
                getLastJsonFromFile(folderName, ttsaveNo).orElse(new JsonObject()));
    }

    public static AccountMarginQuery getGrpcQueryFromModel(RequestType requestType, AccountMarginModel model) {
        return AccountMarginQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setMember(model.toGrpc().getMember())
                .setAccount(model.toGrpc().getAccount())
                .setMarginCurrency(model.toGrpc().getMarginCurrency())
                .setClearingCurrency(model.toGrpc().getClearingCurrency())
                .setPool(model.toGrpc().getPool())
                .build();
    }

    public static LiquiGroupMarginQuery getGrpcQueryFromModel(RequestType requestType, LiquiGroupMarginModel model) {
        return LiquiGroupMarginQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setMember(model.toGrpc().getMember())
                .setAccount(model.toGrpc().getAccount())
                .setMarginClass(model.toGrpc().getMarginClass())
                .setMarginCurrency(model.toGrpc().getMarginCurrency())
                .setMarginGroup(model.toGrpc().getMarginGroup())
                .build();
    }

    public static LiquiGroupSplitMarginQuery getGrpcQueryFromModel(RequestType requestType, LiquiGroupSplitMarginModel model) {
        return LiquiGroupSplitMarginQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setMember(model.toGrpc().getMember())
                .setAccount(model.toGrpc().getAccount())
                .setLiquidationGroup(model.toGrpc().getLiquidationGroup())
                .setLiquidationGroupSplit(model.toGrpc().getLiquidationGroupSplit())
                .setMarginCurrency(model.toGrpc().getMarginCurrency())
                .build();
    }

    public static PoolMarginQuery getGrpcQueryFromModel(RequestType requestType, PoolMarginModel model) {
        return PoolMarginQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setPool(model.toGrpc().getPool())
                .setMarginCurrency(model.toGrpc().getMarginCurrency())
                .setClrRptCurrency(model.toGrpc().getClrRptCurrency())
                .setPoolOwner(model.toGrpc().getPoolOwner())
                .build();
    }

    public static PositionReportQuery getGrpcQueryFromModel(RequestType requestType, PositionReportModel model) {
        return PositionReportQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setMember(model.toGrpc().getMember())
                .setAccount(model.toGrpc().getAccount())
                .setLiquidationGroup(model.toGrpc().getLiquidationGroup())
                .setLiquidationGroupSplit(model.toGrpc().getLiquidationGroupSplit())
                .setProduct(model.toGrpc().getProduct())
                .setCallPut(model.toGrpc().getCallPut())
                .setContractYear(model.toGrpc().getContractYear())
                .setContractMonth(model.toGrpc().getContractMonth())
                .setExpiryDay(model.toGrpc().getExpiryDay())
                .setExercisePrice(model.toGrpc().getExercisePrice())
                .setVersion(model.toGrpc().getVersion())
                .setFlexContractSymbol(model.toGrpc().getFlexContractSymbol())
                .setClearingCurrency(model.toGrpc().getClearingCurrency())
                .setProductCurrency(model.toGrpc().getProductCurrency())
                .setUnderlying(model.toGrpc().getUnderlying())
                .build();
    }

    public static RiskLimitUtilizationQuery getGrpcQueryFromModel(RequestType requestType, RiskLimitUtilizationModel model) {
        return RiskLimitUtilizationQuery.newBuilder()
                .setLatest(requestType == RequestType.LATEST)
                .setClearer(model.toGrpc().getClearer())
                .setMember(model.toGrpc().getMember())
                .setMaintainer(model.toGrpc().getMaintainer())
                .setLimitType(model.toGrpc().getLimitType())
                .build();
    }

    public static int getJsonObjectCount(String folderName, int ttsaveNo) {
        return getJsonArrayFromTTSaveFile(folderName, ttsaveNo)
                .orElse(new JsonArray())
                .size();
    }
}