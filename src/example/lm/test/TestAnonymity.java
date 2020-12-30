package lm.test;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.StatisticsEquivalenceClasses;
import org.deidentifier.arx.criteria.*;
import org.deidentifier.arx.framework.check.groupify.HashGroupifyDistribution;
import org.deidentifier.arx.metric.Metric;
import org.deidentifier.arx.risk.RiskEstimateBuilder;
import org.deidentifier.arx.risk.RiskModelSampleRisks;
import org.deidentifier.arx.risk.RiskModelSampleSummary;
import smile.data.Dataset;

import java.util.Iterator;


/**
 * @className : TestDataSourceARX
 * @Author : liming Mu
 * @Date : 2020/11/23
 * @Version 1.0
 * @Description : 测试匿名化
 */
public class TestAnonymity {

    public static void main(String[] args) throws Exception {
        DataSource jdbcSource = DataSource.createJDBCSource("jdbc:sqlserver://10.245.44.136:1432;DatabaseName=TestDB;selectMethod=cursor",
                "sa",
                "Nsfocus123",
                "TestDB",
                "testc",
                "alldata3",
                4);
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

        AttributeType.Hierarchy.DefaultHierarchy sex = AttributeType.Hierarchy.create();
        sex.add("男", "保密");
        sex.add("女", "保密");

        AttributeType.Hierarchy.DefaultHierarchy passportid = AttributeType.Hierarchy.create();
        passportid.add("MA2203069", "M","*");
        passportid.add("D45171383", "M","*");
        passportid.add("e17920400", "M","*");
        passportid.add("d36865304", "D","*");
        passportid.add("ED2430936", "D","*");
        passportid.add("kj0593141", "K","*");
        passportid.add("152096920", "K","*");
        passportid.add("Pe1999145", "P","*");
        passportid.add("K40164485", "K","*");
        passportid.add("153985537", "L","*");
        passportid.add("146876344", "L","*");
        passportid.add("149478768", "L","*");

        HierarchyBuilderIntervalBased<Double> IMSI_H = HierarchyBuilderIntervalBased.create(DataType.DECIMAL);
        IMSI_H.addInterval(0.0d,1.8d,"very low");
        IMSI_H.addInterval(1.8d, 2.6d, "low");
        IMSI_H.addInterval(2.6d, 3.4d, "normal");
        IMSI_H.addInterval(3.4d, 4.1d, "borderline high");
        IMSI_H.addInterval(4.1d, 4.9d, "high");
        IMSI_H.addInterval(4.9d, 10d, "very high");

        IMSI_H.getLevel(0).addGroup(2, "low").addGroup(2, "norm").addGroup(2, "high");
        IMSI_H.getLevel(1).addGroup(2, "low-norm").addGroup(1, "high");


        data.getDefinition().setAttributeType("id", AttributeType.IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("passportid", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("address", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("Driver_license_number", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IMSI", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("sex", sex);
        data.getDefinition().setAttributeType("Native_place", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("Business_license", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("name", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("email", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("id_number", AttributeType.INSENSITIVE_ATTRIBUTE);

        ARXConfiguration config = ARXConfiguration.create();
        // 准标识符的K值调节
        config.addPrivacyModel(new KAnonymity(1));

        // 隐私模型的策略配置
//        config.addPrivacyModel(new DDisclosurePrivacy("passportid",19d));

     //   config.addPrivacyModel(new EqualDistanceTCloseness("address",2));

        config.setSuppressionLimit(0.5);


        ARXAnonymizer anonymizer = new ARXAnonymizer();
      /*  anonymizer.setMaximumSnapshotSizeDataset(0.2);
        anonymizer.setMaximumSnapshotSizeSnapshot(0.2);
        anonymizer.setHistorySize(1);*/

        ARXResult result = anonymizer.anonymize(data, config);
        DataHandle output = result.getOutput();
        Iterator<String[]> iterator = output.iterator();
        while (iterator.hasNext()) {
            String[] next = iterator.next();
            for (int i = 0; i < next.length; i++) {
                String string = next[i];
                System.out.print(string);
                if (i < next.length - 1) {
                    System.out.print(", ");
                }
            }
            System.out.println();
        }
        System.out.println(result.getOptimumFound());
    }
}
