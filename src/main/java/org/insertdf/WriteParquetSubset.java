//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.StructType;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Arrays;
//import java.util.stream.Collectors;
//
//public class WriteParquetSubset {
//    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder()
//                .appName("WriteParquetSubset")
//                .master("local[*]")
//                .getOrCreate();
//
//        // Существующий путь к таблице Parquet
//        String parquetPath = "path/to/your/existing_table.parquet";
//
//        // Читаем схему существующей таблицы Parquet
//        Dataset<Row> existingDF = spark.read().parquet(parquetPath);
//        StructType existingSchema = existingDF.schema();
//        List<String> existingColumns = Arrays.asList(existingSchema.fieldNames());
//
//        // Создаем DataFrame с меньшим количеством полей
//        List<Row> data = new ArrayList<>();
//        data.add(Row.create(1, "Alice"));
//        data.add(Row.create(2, "Bob"));
//
//        StructType newSchema = new StructType()
//                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType)
//                .add("name", org.apache.spark.sql.types.DataTypes.StringType);
//
//        Dataset<Row> newDF = spark.createDataFrame(data, newSchema);
//        List<String> newColumns = Arrays.asList(newSchema.fieldNames());
//
//        // Создаем новый DataFrame с полями из существующей схемы
//        List<Row> alignedData = newDF.collectAsList().stream()
//                .map(row -> {
//                    List<Object> values = existingColumns.stream()
//                            .map(column -> {
//                                if (newColumns.contains(column)) {
//                                    return row.getAs(column);
//                                } else {
//                                    return null; // Заполняем отсутствующие столбцы null
//                                }
//                            })
//                            .collect(Collectors.toList());
//                    return Row.create(values.toArray());
//                })
//                .collect(Collectors.toList());
//
//        Dataset<Row> alignedDF = spark.createDataFrame(alignedData, existingSchema);
//
//        // Записываем выровненный DataFrame в существующую таблицу Parquet (режим append)
//        alignedDF.write()
//                .mode("append")
//                .parquet(parquetPath);
//
//        spark.stop();
//    }
//}