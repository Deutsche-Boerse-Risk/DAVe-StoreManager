package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class AccountMarginModel extends AbstractModel {

    public AccountMarginModel() {
        // Empty constructor
    }

    public AccountMarginModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("member", String.class);
        keys.put("account", String.class);
        keys.put("marginCurrency", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getUniqueFieldsDescriptor() {
        Map<String, Class<?>> uniqueFields = new LinkedHashMap<>();
        uniqueFields.put("clearingCurrency", String.class);
        uniqueFields.put("pool", String.class);
        return Collections.unmodifiableMap(uniqueFields);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        Map<String, Class<?>> nonKeys = new LinkedHashMap<>();
        nonKeys.put("marginReqInMarginCurr", Double.class);
        nonKeys.put("marginReqInClrCurr", Double.class);
        nonKeys.put("unadjustedMarginRequirement", Double.class);
        nonKeys.put("variationPremiumPayment", Double.class);
        return Collections.unmodifiableMap(nonKeys);
    }
}
