package com.deutscheboerse.risk.dave.mongo.codec;

import com.deutscheboerse.risk.dave.model.*;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class ModelCodecProvider implements CodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == AccountMarginModel.class) {
            return (Codec<T>)new ModelCodec<>(AccountMarginModel::buildFromJson, AccountMarginModel.class);
        }

        if (clazz == LiquiGroupMarginModel.class) {
            return (Codec<T>)new ModelCodec<>(LiquiGroupMarginModel::buildFromJson, LiquiGroupMarginModel.class);
        }

        if (clazz == LiquiGroupSplitMarginModel.class) {
            return (Codec<T>)new ModelCodec<>(LiquiGroupSplitMarginModel::buildFromJson, LiquiGroupSplitMarginModel.class);
        }

        if (clazz == PoolMarginModel.class) {
            return (Codec<T>)new ModelCodec<>(PoolMarginModel::buildFromJson, PoolMarginModel.class);
        }

        if (clazz == PositionReportModel.class) {
            return (Codec<T>)new ModelCodec<>(PositionReportModel::buildFromJson, PositionReportModel.class);
        }

        if (clazz == RiskLimitUtilizationModel.class) {
            return (Codec<T>)new ModelCodec<>(RiskLimitUtilizationModel::buildFromJson, RiskLimitUtilizationModel.class);
        }

        return null;
    }
}
