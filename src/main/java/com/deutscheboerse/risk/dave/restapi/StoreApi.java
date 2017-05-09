package com.deutscheboerse.risk.dave.restapi;

import com.deutscheboerse.risk.dave.*;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.persistence.PersistenceService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.grpc.GrpcReadStream;
import io.vertx.serviceproxy.ProxyHelper;

import java.util.ArrayList;
import java.util.List;

public class StoreApi {
    private static final Logger LOG = LoggerFactory.getLogger(StoreApi.class);
    private static final int DEFAULT_PROXY_SEND_TIMEOUT = 60000;

    protected final Vertx vertx;
    private final PersistenceService persistenceProxy;

    public StoreApi(Vertx vertx) {
        this.vertx = vertx;
        DeliveryOptions deliveryOptions = new DeliveryOptions().setSendTimeout(DEFAULT_PROXY_SEND_TIMEOUT);
        this.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS, deliveryOptions);
    }

    public void storeAccountMargin(GrpcReadStream<AccountMargin> request, Future<StoreReply> response) {
        List<AccountMarginModel> models = new ArrayList<>();
        request.handler(payload -> {
            AccountMarginModel model = this.getAccountMarginModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storeAccountMargin(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    public void storeLiquiGroupMargin(GrpcReadStream<LiquiGroupMargin> request, Future<StoreReply> response) {
        List<LiquiGroupMarginModel> models = new ArrayList<>();
        request.handler(payload -> {
            LiquiGroupMarginModel model = this.getLiquiGroupMarginModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storeLiquiGroupMargin(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    public void storeLiquiGroupSplitMargin(GrpcReadStream<LiquiGroupSplitMargin> request, Future<StoreReply> response) {
        List<LiquiGroupSplitMarginModel> models = new ArrayList<>();
        request.handler(payload -> {
            LiquiGroupSplitMarginModel model = this.getLiquiGroupSplitMarginModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storeLiquiGroupSplitMargin(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    public void storePoolMargin(GrpcReadStream<PoolMargin> request, Future<StoreReply> response) {
        List<PoolMarginModel> models = new ArrayList<>();
        request.handler(payload -> {
            PoolMarginModel model = this.getPoolMarginModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storePoolMargin(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    public void storePositionReport(GrpcReadStream<PositionReport> request, Future<StoreReply> response) {
        List<PositionReportModel> models = new ArrayList<>();
        request.handler(payload -> {
            PositionReportModel model = this.getPositionReportModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storePositionReport(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    public void storeRiskLimitUtilization(GrpcReadStream<RiskLimitUtilization> request, Future<StoreReply> response) {
        List<RiskLimitUtilizationModel> models = new ArrayList<>();
        request.handler(payload -> {
            RiskLimitUtilizationModel model = this.getRiskLimitUtilizationModelFromGpb(payload);
            model.validate();
            models.add(model);
        }).endHandler(v -> {
            this.persistenceProxy.storeRiskLimitUtilization(models, this.getResponseHandler(response));
        }).exceptionHandler(t -> {
            response.fail(t);
        });
    }

    private AccountMarginModel getAccountMarginModelFromGpb(AccountMargin payload) {
        AccountMarginModel model = new AccountMarginModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("member", payload.getMember());
        model.put("account", payload.getAccount());
        model.put("marginCurrency", payload.getMarginCurrency());
        model.put("clearingCurrency", payload.getClearingCurrency());
        model.put("pool", payload.getPool());
        model.put("marginReqInMarginCurr", payload.getMarginReqInMarginCurr());
        model.put("marginReqInClrCurr", payload.getMarginReqInClrCurr());
        model.put("unadjustedMarginRequirement", payload.getUnadjustedMarginRequirement());
        model.put("variationPremiumPayment", payload.getVariationPremiumPayment());
        return model;
    }

    private LiquiGroupMarginModel getLiquiGroupMarginModelFromGpb(LiquiGroupMargin payload) {
        LiquiGroupMarginModel model = new LiquiGroupMarginModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("member", payload.getMember());
        model.put("account", payload.getAccount());
        model.put("marginClass", payload.getMarginClass());
        model.put("marginCurrency", payload.getMarginCurrency());
        model.put("marginGroup", payload.getMarginGroup());
        model.put("premiumMargin", payload.getPremiumMargin());
        model.put("currentLiquidatingMargin", payload.getCurrentLiquidatingMargin());
        model.put("futuresSpreadMargin", payload.getFuturesSpreadMargin());
        model.put("additionalMargin", payload.getAdditionalMargin());
        model.put("unadjustedMarginRequirement", payload.getUnadjustedMarginRequirement());
        model.put("variationPremiumPayment", payload.getVariationPremiumPayment());
        return model;
    }

    private LiquiGroupSplitMarginModel getLiquiGroupSplitMarginModelFromGpb(LiquiGroupSplitMargin payload) {
        LiquiGroupSplitMarginModel model = new LiquiGroupSplitMarginModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("member", payload.getMember());
        model.put("account", payload.getAccount());
        model.put("liquidationGroup", payload.getLiquidationGroup());
        model.put("liquidationGroupSplit", payload.getLiquidationGroupSplit());
        model.put("marginCurrency", payload.getMarginCurrency());
        model.put("premiumMargin", payload.getPremiumMargin());
        model.put("marketRisk", payload.getMarketRisk());
        model.put("liquRisk", payload.getLiquRisk());
        model.put("longOptionCredit", payload.getLongOptionCredit());
        model.put("variationPremiumPayment", payload.getVariationPremiumPayment());
        return model;
    }

    private PoolMarginModel getPoolMarginModelFromGpb(PoolMargin payload) {
        PoolMarginModel model = new PoolMarginModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("pool", payload.getPool());
        model.put("marginCurrency", payload.getMarginCurrency());
        model.put("clrRptCurrency", payload.getClrRptCurrency());
        model.put("requiredMargin", payload.getRequiredMargin());
        model.put("cashCollateralAmount", payload.getCashCollateralAmount());
        model.put("adjustedSecurities", payload.getAdjustedSecurities());
        model.put("adjustedGuarantee", payload.getAdjustedGuarantee());
        model.put("overUnderInMarginCurr", payload.getOverUnderInMarginCurr());
        model.put("overUnderInClrRptCurr", payload.getOverUnderInClrRptCurr());
        model.put("variPremInMarginCurr", payload.getVariPremInMarginCurr());
        model.put("adjustedExchangeRate", payload.getAdjustedExchangeRate());
        model.put("poolOwner", payload.getPoolOwner());
        return model;
    }

    private PositionReportModel getPositionReportModelFromGpb(PositionReport payload) {
        PositionReportModel model = new PositionReportModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("member", payload.getMember());
        model.put("account", payload.getAccount());
        model.put("liquidationGroup", payload.getLiquidationGroup());
        model.put("liquidationGroupSplit", payload.getLiquidationGroupSplit());
        model.put("product", payload.getProduct());
        model.put("callPut", payload.getCallPut());
        model.put("contractYear", payload.getContractYear());
        model.put("contractMonth", payload.getContractMonth());
        model.put("expiryDay", payload.getExpiryDay());
        model.put("exercisePrice", payload.getExercisePrice());
        model.put("version", payload.getVersion());
        model.put("flexContractSymbol", payload.getFlexContractSymbol());
        model.put("netQuantityLs", payload.getNetQuantityLs());
        model.put("netQuantityEa", payload.getNetQuantityEa());
        model.put("clearingCurrency", payload.getClearingCurrency());
        model.put("mVar", payload.getMVar());
        model.put("compVar", payload.getCompVar());
        model.put("compCorrelationBreak", payload.getCompCorrelationBreak());
        model.put("compCompressionError", payload.getCompCompressionError());
        model.put("compLiquidityAddOn", payload.getCompLiquidityAddOn());
        model.put("compLongOptionCredit", payload.getCompLongOptionCredit());
        model.put("productCurrency", payload.getProductCurrency());
        model.put("variationPremiumPayment", payload.getVariationPremiumPayment());
        model.put("premiumMargin", payload.getPremiumMargin());
        model.put("normalizedDelta", payload.getNormalizedDelta());
        model.put("normalizedGamma", payload.getNormalizedGamma());
        model.put("normalizedVega", payload.getNormalizedVega());
        model.put("normalizedRho", payload.getNormalizedRho());
        model.put("normalizedTheta", payload.getNormalizedTheta());
        model.put("underlying", payload.getUnderlying());
        return model;
    }

    private RiskLimitUtilizationModel getRiskLimitUtilizationModelFromGpb(RiskLimitUtilization payload) {
        RiskLimitUtilizationModel model = new RiskLimitUtilizationModel();
        model.put("snapshotID", payload.getSnapshotId());
        model.put("businessDate", payload.getBusinessDate());
        model.put("timestamp", payload.getTimestamp());
        model.put("clearer", payload.getClearer());
        model.put("member", payload.getMember());
        model.put("maintainer", payload.getMaintainer());
        model.put("limitType", payload.getLimitType());
        model.put("utilization", payload.getUtilization());
        model.put("warningLevel", payload.getWarningLevel());
        model.put("throttleLevel", payload.getThrottleLevel());
        model.put("rejectLevel", payload.getRejectLevel());
        return model;
    }
    
    private Handler<AsyncResult<Void>> getResponseHandler(Future<StoreReply> response) {
        return ar -> {
            if (ar.succeeded()) {
                LOG.trace("Received response for store request");
                response.complete(StoreReply.newBuilder().setSucceeded(true).build());
            } else {
                LOG.error("Failed to store the document", ar.cause());
                response.fail(ar.cause());
            }
        };
    }
}
