package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class RiskLimitUtilizationModel extends AbstractModel {

    public RiskLimitUtilizationModel() {
        // Empty constructor
    }

    public RiskLimitUtilizationModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("member", String.class);
        keys.put("maintainer", String.class);
        keys.put("limitType", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("utilization", Double.class);
        keys.put("warningLevel", Double.class);
        keys.put("throttleLevel", Double.class);
        keys.put("rejectLevel", Double.class);
        return Collections.unmodifiableMap(keys);
    }
}
