package com.deutscheboerse.risk.dave.mongo.model;

import com.deutscheboerse.risk.dave.model.RiskLimitUtilizationModel;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.ext.mongo.UpdateOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class UpdateOneModelTest {

    @Test
    public void testUpsert(TestContext context) {
        RiskLimitUtilizationModel model = DataHelper.getLastModelFromFile(
                DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1, RiskLimitUtilizationModel::buildFromJson);
        UpdateOneModel<RiskLimitUtilizationModel> updateModel = new UpdateOneModel<>(model, new UpdateOptions().setUpsert(true));
        context.assertTrue(updateModel.getMongoWriteModel().getOptions().isUpsert());
    }

    @Test
    public void testNoUpsert(TestContext context) {
        RiskLimitUtilizationModel model = DataHelper.getLastModelFromFile(
                DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, 1, RiskLimitUtilizationModel::buildFromJson);
        UpdateOneModel<RiskLimitUtilizationModel> updateModel = new UpdateOneModel<>(model, new UpdateOptions().setUpsert(false));
        context.assertFalse(updateModel.getMongoWriteModel().getOptions().isUpsert());
    }
}
