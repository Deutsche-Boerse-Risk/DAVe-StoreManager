package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class LiquiGroupMarginModel extends AbstractModel {

    public LiquiGroupMarginModel() {
        // Empty constructor
    }

    public LiquiGroupMarginModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("member", String.class);
        keys.put("account", String.class);
        keys.put("marginClass", String.class);
        keys.put("marginCurrency", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        Map<String, Class<?>> nonKeys = new LinkedHashMap<>();
        nonKeys.put("marginGroup", String.class);
        nonKeys.put("premiumMargin", Double.class);
        nonKeys.put("currentLiquidatingMargin", Double.class);
        nonKeys.put("futuresSpreadMargin", Double.class);
        nonKeys.put("additionalMargin", Double.class);
        nonKeys.put("unadjustedMarginRequirement", Double.class);
        nonKeys.put("variationPremiumPayment", Double.class);
        return Collections.unmodifiableMap(nonKeys);
    }
}
