//package org.example;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;
//
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//import static org.apache.spark.sql.functions.col;
//
//
////TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
//// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
//public class Main2 {
//    public static void main1(String[] args) {
//
//
//        SparkSession ss = SparkSession.builder()
//                .appName("ExcelToDataFrame")
//                .master("local[*]")
//                .getOrCreate();
////        ss.sparkContext().setLogLevel("ERROR");
//
//        // Чтение Excel-файла в DataFrame
//        Dataset<Row> df = ss.read()
//                .format("com.crealytics.spark.excel")
//                .option("header", "true") // Указывает, что первая строка содержит заголовки
//                .load("C:\\Users\\Тимур\\IdeaProjects\\SparkColumns\\src\\main\\resources\\columns.xlsx");
//
//        // Показать содержимое DataFrame
//        df.show(100);
//
//        Dataset<Row> df1 = df.filter(col("name").equalTo("name"));
//        df1.show();
//        StructType schema = new StructType()
//                .add("tmp", DataTypes.IntegerType, true) ;// Столбец id (Integer, допускает null)
//
//        // Сбор данных на драйвере
//        List<Row> rows = df1.collectAsList();
//
//        // Перебор строк
//        for (Row row : rows) {
//            Dataset<Row> df2 = df
//                    .filter("source_file = '" + row.getAs("source_file") +"' and name != 'name'");
//            System.out.println(row);
//            df2.show();
//            Set<String> excludedColumns = new HashSet<>(Arrays.asList("source_file", "name", "id", "org"));
//            createDF (df2, excludedColumns);
//
//        }
//
//// Задержка перед завершением (например, 5 минут)
//        try {
//            Thread.sleep(5); // 5 минут
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        // Остановка Spark-сессии
//        ss.stop();
//
//    }
//
//    public static void createDF (Dataset df, Set<String> excludedColumns) {
//
//        String[] dynamicColumns = Arrays.stream(df.columns())
//                .filter(col ->  !excludedColumns.contains(col)) // Исключаем столбец ID
//                .toArray(String[]::new);
//
//        System.out.println(excludedColumns.toString());
//
//        String stackExpr = Arrays.stream(columns)
//                .map(col -> "'" + col + "', " + col)
//                .collect(Collectors.joining(", "));
//
//        String stackQuery = String.format("stack(%d, %s) as (ques_id, ans)", columns.length, stackExpr);
//
////        String cols = "";
////        for (String col: excludedColumns){
////            cols = "'" + col + "'," + cols  ;
////        }
//        List <String> staticColumns = Arrays.stream()
////        cols = cols.substring(0, cols.length() - 1);
////        cols = "\'org\', \'name\', \'id\', \'source_file\', " + stackQuery;
//
//
//        System.out.println(stackQuery);
//
//        // Выполнение melt через динамический stack
//        df = df.selectExpr(cols)
//                .filter("Ans is not NULL");
//
//        // Результат
//        System.out.println("Результат после динамического melt:");
//        df.show();
//    }
//}
//
