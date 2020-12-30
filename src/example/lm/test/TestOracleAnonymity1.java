package lm.test;

import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.criteria.KAnonymity;

import java.util.Iterator;


/**
 * @className : TestDataSourceARX
 * @Author : liming Mu
 * @Date : 2020/11/23
 * @Version 1.0
 * @Description : 根据数据自动构建泛化树，输出匿名化结果
 */
public class TestOracleAnonymity1 {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        //1 创建数据源
        DataSource jdbcSource = DataSource.createJDBCSource("jdbc:oracle:thin:@//10.245.44.111:1521/orcl11g",
                "YPK1",
                "123456",
                "YPK1",
                "YPK1",
                "T0",
                100000);
        //2 添加纳入运算的列（不一定是全列）
        jdbcSource.addColumn("ID", DataType.STRING);
        jdbcSource.addColumn("NAME", DataType.STRING);
        jdbcSource.addColumn("IDCARD", DataType.STRING);
        jdbcSource.addColumn("PHONE", DataType.STRING);
        jdbcSource.addColumn("TEL", DataType.STRING);
        jdbcSource.addColumn("BANKCARD", DataType.STRING);
        jdbcSource.addColumn("ADDRESS", DataType.STRING);
        jdbcSource.addColumn("PASSPORT", DataType.STRING);
        jdbcSource.addColumn("ARMY_OFFICER", DataType.STRING);
        jdbcSource.addColumn("TEL2", DataType.STRING);
        jdbcSource.addColumn("SOCIAL_CODE", DataType.STRING);
        jdbcSource.addColumn("EMAIL", DataType.STRING);
        jdbcSource.addColumn("COMPANY", DataType.STRING);
        jdbcSource.addColumn("URL", DataType.STRING);
        jdbcSource.addColumn("IDCARD2", DataType.STRING);
        jdbcSource.addColumn("PHONE2", DataType.STRING);
        jdbcSource.addColumn("BANKCARD2", DataType.STRING);
        jdbcSource.addColumn("PASSPORT2", DataType.STRING);
        jdbcSource.addColumn("NAME2", DataType.STRING);
        jdbcSource.addColumn("ARMY_OFFICER2", DataType.STRING);

        //3 创建数据
        Data data = Data.create(jdbcSource);

        //4 拿到handle，即读取数据后的结果
        DataHandle handle = data.getHandle();

        //5 构建准标识符的泛化树
        //5.1 获取准标识符的种类
        HierarchyBuilderRedactionBased<?> builder_idcard = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        //5.2 得到所有的值
        String[] idcard_s =handle.getDistinctValues(handle.getColumnIndexOf("IDCARD"));

        //5.3 泛化
        builder_idcard.prepare(idcard_s);
        String[][] hierarchy_idcard = builder_idcard.build().getHierarchy();
//        printArray(hierarchy);
        //5.4 构建泛化树
        AttributeType.Hierarchy.DefaultHierarchy idcard = AttributeType.Hierarchy.create();
        addHierarchy(idcard,hierarchy_idcard);

        //--------------------------------------
        HierarchyBuilderRedactionBased<?> builder_phone = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        String[] phone_s =handle.getDistinctValues(handle.getColumnIndexOf("PHONE"));
        builder_phone.prepare(phone_s);
        String[][] hierarchy_phone = builder_phone.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy phone = AttributeType.Hierarchy.create();
        addHierarchy(phone,hierarchy_phone);


        //--------------------------------------
        HierarchyBuilderRedactionBased<?> builder_addresses = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        String[] addresses_s =handle.getDistinctValues(handle.getColumnIndexOf("ADDRESS"));
        builder_addresses.prepare(addresses_s);
        String[][] hierarchy_addresses = builder_addresses.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy addresses = AttributeType.Hierarchy.create();
        addHierarchy(addresses,hierarchy_addresses);

        //6 配置列的敏感类型。（不配置的， 默认是身份标志符）
        data.getDefinition().setAttributeType("ID", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("NAME", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IDCARD",idcard);
        data.getDefinition().setAttributeType("PHONE", phone);
        data.getDefinition().setAttributeType("TEL", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("BANKCARD", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("ADDRESS", addresses);
        data.getDefinition().setAttributeType("PASSPORT", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("ARMY_OFFICER", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("TEL2",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("SOCIAL_CODE", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("EMAIL", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("COMPANY", AttributeType.IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("URL", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IDCARD2", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("PHONE2",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("BANKCARD2", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("PASSPORT2", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("NAME2", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("ARMY_OFFICER2", AttributeType.INSENSITIVE_ATTRIBUTE);

        //7 创建配置文件，即匿名化的各个参数指标（主要配置敏感属性的处理策略，以及准标识符的k值）
        ARXConfiguration config = ARXConfiguration.create();
        //7.1 准标识符的K值调节
        config.addPrivacyModel(new KAnonymity(2));

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

        long end = System.currentTimeMillis();
        System.out.println("spend total time : " + (end-start)/1000 + " s");
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
