package com.deutscheboerse.risk.dave.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@DataObject
public class PositionReportModel extends AbstractModel {

    public PositionReportModel() {
        // Empty constructor
    }

    public PositionReportModel(JsonObject json) {
        super(json);
    }

    @Override
    public Map<String, Class> getKeysDescriptor() {
        HashMap<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("clearer", String.class);
        keys.put("member", String.class);
        keys.put("account", String.class);
        keys.put("liquidationGroup", String.class);
        keys.put("liquidationGroupSplit", String.class);
        keys.put("product", String.class);
        keys.put("callPut", String.class);
        keys.put("contractYear", Integer.class);
        keys.put("contractMonth", Integer.class);
        keys.put("expiryDay", Integer.class);
        keys.put("exercisePrice", Double.class);
        keys.put("version", String.class);
        keys.put("flexContractSymbol", String.class);
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public Map<String, Class> getUniqueFieldsDescriptor() {
        Map<String, Class<?>> uniqueFields = new LinkedHashMap<>();
        uniqueFields.put("clearingCurrency", String.class);
        uniqueFields.put("productCurrency", String.class);
        uniqueFields.put("underlying", String.class);
        return Collections.unmodifiableMap(uniqueFields);
    }

    @Override
    public Map<String, Class> getNonKeysDescriptor() {
        HashMap<String, Class<?>> keys = new LinkedHashMap<>();
        keys.put("netQuantityLs", Double.class);
        keys.put("netQuantityEa", Double.class);
        keys.put("mVar", Double.class);
        keys.put("compVar", Double.class);
        keys.put("compCorrelationBreak", Double.class);
        keys.put("compCompressionError", Double.class);
        keys.put("compLiquidityAddOn", Double.class);
        keys.put("compLongOptionCredit", Double.class);
        keys.put("variationPremiumPayment", Double.class);
        keys.put("premiumMargin", Double.class);
        keys.put("normalizedDelta", Double.class);
        keys.put("normalizedGamma", Double.class);
        keys.put("normalizedVega", Double.class);
        keys.put("normalizedRho", Double.class);
        keys.put("normalizedTheta", Double.class);
        return Collections.unmodifiableMap(keys);
    }
}
