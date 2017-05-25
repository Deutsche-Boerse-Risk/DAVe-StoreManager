package com.deutscheboerse.risk.dave.model;

import com.deutscheboerse.risk.dave.grpc.RiskLimitUtilization;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@DataObject
public class RiskLimitUtilizationModel implements Model<RiskLimitUtilization> {

    private final RiskLimitUtilization grpc;

    public RiskLimitUtilizationModel(RiskLimitUtilization grpc) {
        this.grpc = grpc;
    }

    public RiskLimitUtilizationModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(RiskLimitUtilization.class);
    }

    public static RiskLimitUtilizationModel buildFromJson(JsonObject json) {
        return new RiskLimitUtilizationModel(RiskLimitUtilization.newBuilder()
                .setSnapshotId(json.getInteger("snapshotID"))
                .setBusinessDate(json.getInteger("businessDate"))
                .setTimestamp(json.getLong("timestamp"))
                .setClearer(json.getString("clearer"))
                .setMember(json.getString("member"))
                .setMaintainer(json.getString("maintainer"))
                .setLimitType(json.getString("limitType"))
                .setUtilization(json.getDouble("utilization"))
                .setWarningLevel(json.getDouble("warningLevel", 0.0d))
                .setThrottleLevel(json.getDouble("throttleLevel", 0.0d))
                .setRejectLevel(json.getDouble("rejectLevel", 0.0d))
                .build());
    }

    @Override
    public RiskLimitUtilization toGrpc() {
        return this.grpc;
    }

    @Override
    public JsonObject getMongoQueryParams() {
        JsonObject queryParams = new JsonObject();
        queryParams.put("clearer", this.grpc.getClearer());
        queryParams.put("member", this.grpc.getMember());
        queryParams.put("maintainer", this.grpc.getMaintainer());
        queryParams.put("limitType", this.grpc.getLimitType());
        return queryParams;
    }

    @Override
    public JsonObject getMongoStoreDocument() {
        JsonObject document = new JsonObject();
        JsonObject snapshotDocument = new JsonObject();
        snapshotDocument.put("snapshotID", this.grpc.getSnapshotId());
        snapshotDocument.put("businessDate", this.grpc.getBusinessDate());
        snapshotDocument.put("timestamp", this.grpc.getTimestamp());
        snapshotDocument.put("utilization", this.grpc.getUtilization());
        snapshotDocument.put("warningLevel", this.grpc.getWarningLevel());
        snapshotDocument.put("throttleLevel", this.grpc.getThrottleLevel());
        snapshotDocument.put("rejectLevel", this.grpc.getRejectLevel());
        document.put("$set", this.getMongoQueryParams());
        document.put("$addToSet", new JsonObject().put("snapshots", snapshotDocument));
        return document;
    }

    public static MongoModelDescriptor getMongoModelDescriptor() {
        return new MongoModelDescriptor() {
            @Override
            public String getCollectionName() {
                return "RiskLimitUtilization";
            }

            @Override
            public Collection<String> getCommonFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "clearer",
                        "member",
                        "maintainer",
                        "limitType"
                ));
            }

            @Override
            public Collection<String> getSnapshotFields() {
                return Collections.unmodifiableList(Arrays.asList(
                        "snapshotID",
                        "businessDate",
                        "timestamp",
                        "utilization",
                        "warningLevel",
                        "throttleLevel",
                        "rejectLevel"
                ));
            }
        };
    }
}
