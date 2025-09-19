package org.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.*;


public class Main {
    public static void main(String[] args) {


        SparkSession ss = SparkSession.builder()
                .appName("ExcelToDataFrame")
                .master("local[*]")
                .getOrCreate();

        // Чтение Excel-файла в DataFrame
        Dataset<Row> df = ss.read()
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .load("C:\\Users\\Тимур\\IdeaProjects\\SparkColumns\\src\\main\\resources\\columns.xlsx");

        Dataset<Row> columnsNamesDf = df.filter("name='name'");

//        columnsNamesDf.show();

//        columnsNamesDf.toLocalIterator().forEachRemaining(row -> {
          for (Row row : columnsNamesDf.collectAsList()) {

                // Обработка каждой строки
            Dataset<Row> vkcPortionDf = df
                    .filter("source_file = '" + row.getAs("source_file") + "' and name != 'name'");
//            System.out.println(row);
//            vkcPortionDf.show();

            if (columnsNamesDf.isEmpty()) {
                System.err.println("Ошибка: В файле нет строк с name='name'.");
                return;
            }

            List<String> staticColumns = new ArrayList<>(Arrays.asList("source_file", "name", "id", "org"));
            List<String> dinamicColumns = new ArrayList<>(Arrays.asList("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"));


              HashMap<String, String> renCols = new HashMap<String, String>();
            int numColumns = columnsNamesDf.columns().length;
            for (int i = 0; i < numColumns; ++i) {
                renCols.put(vkcPortionDf.columns()[i], row.getString(i));
            }

            TransformDF transformedDF = new TransformDF(vkcPortionDf);
            transformedDF.renameColumns(renCols, staticColumns, dinamicColumns);
            transformedDF.unpivotDf(staticColumns);

            transformedDF.getDf().show();
        }

        ss.stop();

    }

    public static void Func(){
        System.out.println("ddddd");
    }

}

