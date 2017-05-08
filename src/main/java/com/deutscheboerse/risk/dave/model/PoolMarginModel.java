package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class PoolMarginModel extends AbstractModel {

    public PoolMarginModel() {
        // Empty constructor
    }

    public PoolMarginModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("pool", String.class);
        keys.put("marginCurrency", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getUniqueFieldsDescriptor() {
        Map<String, Class<?>> uniqueFields = new LinkedHashMap<>();
        uniqueFields.put("clrRptCurrency", String.class);
        uniqueFields.put("poolOwner", String.class);
        return Collections.unmodifiableMap(uniqueFields);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("requiredMargin", Double.class);
        keys.put("cashCollateralAmount", Double.class);
        keys.put("adjustedSecurities", Double.class);
        keys.put("adjustedGuarantee", Double.class);
        keys.put("overUnderInMarginCurr", Double.class);
        keys.put("overUnderInClrRptCurr", Double.class);
        keys.put("variPremInMarginCurr", Double.class);
        keys.put("adjustedExchangeRate", Double.class);
        return Collections.unmodifiableMap(keys);
    }
}
