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
public class TestMysqlAnonymity1 {

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        //1 创建数据源
        DataSource jdbcSource = DataSource.createJDBCSource("jdbc:mysql://10.66.38.175:3306/ personalrecords?serverTimezone=Asia/Shanghai&characterEncoding=UTF-8",
                "root",
                "nsfocus123",
                "personalrecords",
                "personalrecords",
                "10w",
                100000);
        //2 添加纳入运算的列（不一定是全列）
        jdbcSource.addColumn("idcard", DataType.STRING);
        jdbcSource.addColumn("name", DataType.STRING);
        jdbcSource.addColumn("ename", DataType.STRING);
        jdbcSource.addColumn("sex", DataType.STRING);
        jdbcSource.addColumn("birthday", DataType.STRING);
        jdbcSource.addColumn("age", DataType.STRING);
        jdbcSource.addColumn("jiGuan", DataType.STRING);
        jdbcSource.addColumn("junGuanNum", DataType.STRING);
        jdbcSource.addColumn("passport", DataType.STRING);
        jdbcSource.addColumn("jiazhaoNum", DataType.STRING);
        jdbcSource.addColumn("carNum", DataType.STRING);
        jdbcSource.addColumn("IMSI", DataType.STRING);
        jdbcSource.addColumn("address", DataType.STRING);
        jdbcSource.addColumn("postcode", DataType.STRING);
        jdbcSource.addColumn("phone", DataType.STRING);
        jdbcSource.addColumn("email", DataType.STRING);
        jdbcSource.addColumn("QQ", DataType.STRING);
        jdbcSource.addColumn("wechat", DataType.STRING);
        jdbcSource.addColumn("bankAccount", DataType.STRING);
        jdbcSource.addColumn("company", DataType.STRING);
        jdbcSource.addColumn("badge", DataType.STRING);
        jdbcSource.addColumn("job", DataType.STRING);
        jdbcSource.addColumn("department", DataType.STRING);
        jdbcSource.addColumn("entryTime", DataType.STRING);
        jdbcSource.addColumn("IPV4", DataType.STRING);
        jdbcSource.addColumn("IPV6", DataType.STRING);
        jdbcSource.addColumn("Mac", DataType.STRING);
        jdbcSource.addColumn("workcity", DataType.STRING);

        //3 创建数据
        Data data = Data.create(jdbcSource);

        //4 拿到handle，即读取数据后的结果
        DataHandle handle = data.getHandle();

        //5 构建准标识符的泛化树
        //5.1 获取准标识符的种类
        HierarchyBuilderRedactionBased<?> builder_birthday = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        //5.2 得到所有的值
        String[] birthday_s =handle.getDistinctValues(handle.getColumnIndexOf("birthday"));

        //5.3 泛化
        builder_birthday.prepare(birthday_s);
        String[][] hierarchy_birthday= builder_birthday.build().getHierarchy();
//        printArray(hierarchy);
        //5.4 构建泛化树
        AttributeType.Hierarchy.DefaultHierarchy birthday = AttributeType.Hierarchy.create();
        addHierarchy(birthday,hierarchy_birthday);

        //--------------------------------------
        HierarchyBuilderRedactionBased<?> builder_jiGuan = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        String[] jiGuan_s =handle.getDistinctValues(handle.getColumnIndexOf("jiGuan"));
        builder_jiGuan.prepare(jiGuan_s);
        String[][] hierarchy_jiGuan = builder_jiGuan.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy jiGuan = AttributeType.Hierarchy.create();
        addHierarchy(jiGuan,hierarchy_jiGuan);


        //--------------------------------------
        HierarchyBuilderRedactionBased<?> builder_addresses = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
                ' ', '*');
        String[] addresses_s =handle.getDistinctValues(handle.getColumnIndexOf("address"));
        builder_addresses.prepare(addresses_s);
        String[][] hierarchy_addresses = builder_addresses.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy addresses = AttributeType.Hierarchy.create();
        addHierarchy(addresses,hierarchy_addresses);

        //--------------------------------------
        HierarchyBuilderRedactionBased<?> builder_department = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                HierarchyBuilderRedactionBased.Order.LEFT_TO_RIGHT,
                ' ', '*');
        String[] department_s =handle.getDistinctValues(handle.getColumnIndexOf("department"));
        builder_department.prepare(department_s);
        String[][] hierarchy_department = builder_department.build().getHierarchy();
        AttributeType.Hierarchy.DefaultHierarchy department = AttributeType.Hierarchy.create();
        addHierarchy(department,hierarchy_department);

        //6 配置列的敏感类型。（不配置的， 默认是身份标志符）
        data.getDefinition().setAttributeType("idcard", AttributeType.IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("name", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("ename", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("sex", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("birthday", birthday);
        data.getDefinition().setAttributeType("age", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("jiGuan", jiGuan);
        data.getDefinition().setAttributeType("junGuanNum", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("passport", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("jiazhaoNum", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("carNum", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IMSI", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("address", addresses);
        data.getDefinition().setAttributeType("postcode", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("phone", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("email", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("QQ", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("wechat", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("bankAccount", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("company", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("badge", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("job", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("department", department);
        data.getDefinition().setAttributeType("entryTime", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IPV4", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("IPV6", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("Mac", AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("workcity", AttributeType.INSENSITIVE_ATTRIBUTE);

        //7 创建配置文件，即匿名化的各个参数指标（主要配置敏感属性的处理策略，以及准标识符的k值）
        ARXConfiguration config = ARXConfiguration.create();
        //7.1 准标识符的K值调节
        config.addPrivacyModel(new KAnonymity(3));

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
