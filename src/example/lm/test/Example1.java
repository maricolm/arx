package lm.test;

import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.examples.Example;
import org.deidentifier.arx.risk.RiskModelAttributes;
import org.deidentifier.arx.risk.RiskModelSampleRisks;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

/**
 * @className : Example1
 * @Author : liming Mu
 * @Date : 2020/11/23
 * @Version 1.0
 * @Description : TODO
 */
public class Example1 extends Example {

    public static void main(String[] args) throws IOException {

       /* DataSource dataSource = DataSource.createJDBCSource(
                jdbcUrl, jdbcInfoBeanDTO.getUser(),
                jdbcInfoBeanDTO.getPasswd(), dbName, schemaName, tableName, limitNum);

        // 选择哪些列进入处理流程
        Set<String> keys = tableLabelResult.keySet();
        for (String key : keys) {
            dataSource.addColumn(key, DataType.STRING, true);
        }*/

        // Define data
        Data.DefaultData data = Data.create();
        data.add("age", "gender", "zipcode");
        data.add("34", "male", "81667");
        data.add("45", "female", "81675");
        data.add("66", "male", "81925");
        data.add("70", "female", "81931");
        data.add("34", "female", "81931");
        data.add("70", "male", "81931");
        data.add("45", "male", "81931");

        data.add("34", "male", "81667");
        data.add("45", "female", "81675");
        data.add("66", "male", "81925");
        data.add("70", "female", "81931");
        data.add("34", "female", "81931");
        data.add("70", "male", "81931");
        data.add("45", "male", "81931");

        data.add("34", "male", "81667");
        data.add("45", "female", "81675");
        data.add("66", "male", "81925");
        data.add("70", "female", "81931");
        data.add("34", "female", "81931");
        data.add("70", "male", "81931");
        data.add("45", "male", "81931");

        // 设置属性类型
        data.getDefinition().setAttributeType("age", AttributeType.IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("gender", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("zipcode", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);


        // Analyze risks
        DataHandle handle = data.getHandle();
        ARXPopulationModel population = ARXPopulationModel.create(ARXPopulationModel.Region.CHINA);
        RiskModelSampleRisks risks = handle.getRiskEstimator(population).getSampleBasedReidentificationRisk();

        System.out.println(risks.getAverageRisk());
        System.out.println(risks.getHighestRisk());
        System.out.println(risks.getEstimatedJournalistRisk());
        System.out.println(risks.getEstimatedMarketerRisk());
//        检察官风险
        System.out.println(risks.getEstimatedProsecutorRisk());

        System.out.println(risks.getNumRecordsAffectedByLowestRisk());
        System.out.println(risks.getFractionOfRecordsAffectedByLowestRisk());
        System.out.println(risks.getFractionOfRecordsAffectedByHighestRisk());



        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(3));
        config.setSuppressionLimit(0d);

        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        ARXResult result = anonymizer.anonymize(data, config);

        DataHandle output = result.getOutput();
        System.out.println(output.getStatistics());

        // Print info
       /* printResult(result, data);

        // Process results
        System.out.println(" - Transformed data:");
        Iterator<String[]> transformed = result.getOutput(false).iterator();
        while (transformed.hasNext()) {
            System.out.print("   ");
            System.out.println(Arrays.toString(transformed.next()));
        }*/
    }
}
