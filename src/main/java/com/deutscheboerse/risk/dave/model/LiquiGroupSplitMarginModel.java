package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class LiquiGroupSplitMarginModel extends AbstractModel {

    public LiquiGroupSplitMarginModel() {
        // Empty constructor
    }

    public LiquiGroupSplitMarginModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        Map<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("member", String.class);
        keys.put("account", String.class);
        keys.put("liquidationGroup", String.class);
        keys.put("liquidationGroupSplit", String.class);
        keys.put("marginCurrency", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        Map<String, Class<?>> nonKeys = new LinkedHashMap<>();
        nonKeys.put("premiumMargin", String.class);
        nonKeys.put("marketRisk", Double.class);
        nonKeys.put("liquRisk", Double.class);
        nonKeys.put("longOptionCredit", Double.class);
        nonKeys.put("variationPremiumPayment", Double.class);
        return Collections.unmodifiableMap(nonKeys);
    }
}
