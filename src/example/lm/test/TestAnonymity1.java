package lm.test;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import org.deidentifier.arx.criteria.KAnonymity;
import java.util.Iterator;


/**
 * @className : TestDataSourceARX
 * @Author : liming Mu
 * @Date : 2020/11/23
 * @Version 1.0
 * @Description : 根据数据自动构建泛化树，输出匿名化结果
 */
public class TestAnonymity1 {

    public static void main(String[] args) throws Exception {
        //1 创建数据源
        DataSource jdbcSource = DataSource.createJDBCSource("jdbc:sqlserver://10.245.44.136:1432;DatabaseName=TestDB;selectMethod=cursor",
                "sa",
                "Nsfocus123",
                "TestDB",
                "testc",
                "alldata3",
                0);
        //2 添加纳入运算的列（不一定是全列）
        jdbcSource.addColumn("id", DataType.STRING);
        jdbcSource.addColumn("passportid", DataType.STRING);
        jdbcSource.addColumn("Driver_license_number", DataType.STRING);
        jdbcSource.addColumn("IMSI", DataType.STRING);
        jdbcSource.addColumn("sex", DataType.STRING);
        jdbcSource.addColumn("Native_place", DataType.STRING);
        jdbcSource.addColumn("Business_license", DataType.STRING);
        jdbcSource.addColumn("address", DataType.STRING);
      /*  jdbcSource.addColumn("name", DataType.STRING);
        jdbcSource.addColumn("email", DataType.STRING);
        jdbcSource.addColumn("id_number", DataType.STRING);*/

        //3 创建数据
        Data data = Data.create(jdbcSource);

        //4 拿到handle，即读取数据后的结果
        DataHandle handle = data.getHandle();

        //5 构建准标识符的泛化树
        //5.1 获取准标识符的种类
        HierarchyBuilderRedactionBased<?> builder = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        //5.2 得到所有的值
        String[] imsi_s =handle.getDistinctValues(handle.getColumnIndexOf("IMSI"));

        //5.3 泛化
        builder.prepare(imsi_s);
        String[][] hierarchy = builder.build().getHierarchy();
//        printArray(hierarchy);
        //5.4 构建泛化树
        AttributeType.Hierarchy.DefaultHierarchy imsi = AttributeType.Hierarchy.create();
        addHierarchy(imsi,hierarchy);

        HierarchyBuilderRedactionBased<?> builder_sex = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        String[] sex_s =handle.getDistinctValues(handle.getColumnIndexOf("sex"));
        builder_sex.prepare(sex_s);
        String[][] hierarchy_sex = builder_sex.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy sex = AttributeType.Hierarchy.create();
        addHierarchy(sex,hierarchy_sex);


        //6 配置列的敏感类型。（不配置的， 默认是身份标志符）
        data.getDefinition().setAttributeType("id", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("passportid", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("address", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("Driver_license_number", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IMSI", imsi);
        data.getDefinition().setAttributeType("sex", sex);
        data.getDefinition().setAttributeType("Native_place", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("Business_license", AttributeType.INSENSITIVE_ATTRIBUTE);
        /*data.getDefinition().setAttributeType("name", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("email", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("id_number", AttributeType.INSENSITIVE_ATTRIBUTE);*/


        //7 创建配置文件，即匿名化的各个参数指标（主要配置敏感属性的处理策略，以及准标识符的k值）
        ARXConfiguration config = ARXConfiguration.create();
        //7.1 准标识符的K值调节
        config.addPrivacyModel(new KAnonymity(8));

        //7.2 隐私模型的策略配置
//        config.addPrivacyModel(new DDisclosurePrivacy("passportid",19d));
     // config.addPrivacyModel(new EqualDistanceTCloseness("address",2));
      //  config.addPrivacyModel(new DistinctLDiversity("id",5));

        config.setSuppressionLimit(0.5);

        //8 创建匿名对象
        ARXAnonymizer anonymizer = new ARXAnonymizer();
        anonymizer.setMaximumSnapshotSizeDataset(0.2);
        anonymizer.setMaximumSnapshotSizeSnapshot(0.2);
        anonymizer.setHistorySize(1);

        //9 结果输出
        ARXResult result = anonymizer.anonymize(data, config);
        DataHandle output = result.getOutput();
        Iterator<String[]> iterator = output.iterator();
        printRe(iterator);
        System.out.println(result.getOptimumFound());
    }

    // 打印结果集
    private static void printRe(Iterator<String[]> iterator){
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
    }

    //添加泛化
    private static void addHierarchy(AttributeType.Hierarchy.DefaultHierarchy hierarchy, String[][] array) {
        for(String[] next:array){
            hierarchy.add(next);
        }
    }

    //打印泛化树信息
    private static void printArray(String[][] array) {
        System.out.print("{");
        for (int j = 0; j < array.length; j++) {
            String[] next = array[j];
            System.out.print("{");
            for (int i = 0; i < next.length; i++) {
                String string = next[i];
                System.out.print("\"" + string + "\"");
                if (i < next.length - 1) {
                    System.out.print(",");
                }
            }
            System.out.print("}");
            if (j < array.length - 1) {
                System.out.print(",\n");
            }
        }
        System.out.println("}");
    }
}
