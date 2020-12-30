package lm.test;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.StatisticsEquivalenceClasses;
import org.deidentifier.arx.risk.*;

import java.io.IOException;

/**
 * @className : TestRistEvaluate
 * @Author : liming Mu
 * @Date : 2020/12/1
 * @Version 1.0
 * @Description : 测试获取风险评估的指标参数
 */
public class TestRistEvaluate {
    public static void main(String[] args) throws Exception {
        DataSource jdbcSource = DataSource.createJDBCSource("jdbc:sqlserver://10.245.44.136:1432;DatabaseName=TestDB;selectMethod=cursor",
                "sa",
                "Nsfocus123",
                "TestDB",
                "testc",
                "alldata3",
                0);
        jdbcSource.addColumn("id", DataType.STRING);
        jdbcSource.addColumn("passportid", DataType.STRING);
        jdbcSource.addColumn("address", DataType.STRING);
        jdbcSource.addColumn("Driver_license_number", DataType.STRING);
        jdbcSource.addColumn("IMSI", DataType.STRING);
        jdbcSource.addColumn("sex", DataType.STRING);
        jdbcSource.addColumn("Native_place", DataType.STRING);
        jdbcSource.addColumn("Business_license", DataType.STRING);
        jdbcSource.addColumn("name", DataType.STRING);
        jdbcSource.addColumn("email", DataType.STRING);
        jdbcSource.addColumn("id_number", DataType.STRING);

        Data data = Data.create(jdbcSource);

        // 标注准标识符即可
        data.getDefinition().setAttributeType("id", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("sex", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);


        DataHandle handle = data.getHandle();

        RiskModelPopulationUniqueness populationBasedUniquenessRisk = handle.getRiskEstimator().getPopulationBasedUniquenessRisk();
        System.out.println(populationBasedUniquenessRisk.getNumUniqueTuplesZayatz());

        // 定义区域
        ARXPopulationModel populationModel = ARXPopulationModel.create(ARXPopulationModel.Region.CHINA);
        RiskEstimateBuilder builder = handle.getRiskEstimator(populationModel);
        RiskModelSampleSummary risks = builder.getSampleBasedRiskSummary(5d);
        RiskModelSampleRisks sampleReIdentificationRisk = builder.getSampleBasedReidentificationRisk();
        StatisticsEquivalenceClasses equivClasses = handle.getStatistics().getEquivalenceClassStatistics();

        // 获取指标
        // 获取K值， 即准标识符号的最小的K值
        System.out.println(equivClasses.getMinimalEquivalenceClassSize());

        System.out.println(sampleReIdentificationRisk.getAverageRisk());
        System.out.println(risks.getProsecutorRisk().getRecordsAtRisk());
        System.out.println(risks.getJournalistRisk().getRecordsAtRisk());
        System.out.println(risks.getJournalistRisk().getSuccessRate());
        System.out.println(risks.getMarketerRisk().getSuccessRate());

        RiskModelAttributes riskmodel = builder.getAttributeRisks();
        for (RiskModelAttributes.QuasiIdentifierRisk risk : riskmodel.getAttributeRisks()) {
            System.out.println("   * Distinction: " + risk.getDistinction() + ", Separation: " + risk.getSeparation() + ", Identifier: " + risk.getIdentifier());
        }
    }

}
