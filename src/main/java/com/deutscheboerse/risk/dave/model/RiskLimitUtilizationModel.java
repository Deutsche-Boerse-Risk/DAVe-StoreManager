package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    @Override
    protected void validateMissingFields() {
        if ((!containsKey("warningLevel")) && (!containsKey("throttleLevel")) && (!containsKey("rejectLevel"))) {
            throw new IllegalArgumentException("Missing fields in model: 'warningLevel || throttleLevel || rejectLevel'");
        }
        List<String> missingFields = Stream.of(getHeader(), getKeys(), getNonKeys())
                .flatMap(Collection::stream)
                .filter(field -> !"warningLevel".equals(field))
                .filter(field -> !"throttleLevel".equals(field))
                .filter(field -> !"rejectLevel".equals(field))
                .filter(field -> !containsKey(field))
                .collect(Collectors.toList());
        if (!missingFields.isEmpty()) {
            throw new IllegalArgumentException("Missing fields in model: " + missingFields.toString());
        }
    }
}
