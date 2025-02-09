package org.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;
import java.io.File;

import static org.apache.spark.sql.functions.col;
import org.apache.log4j.Logger;


public class ReadParquet {
    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder()
                .appName("E")
                .master("local[*]")
                .getOrCreate();
//        ss.sparkContext().setLogLevel("ERROR");

        // Путь к папке с файлами
        String folderPath = "E:/Datasets/RFSD1";

        // Получаем список всех файлов Parquet
        File folder = new File(folderPath);
        String[] parquetFiles = Arrays.stream(folder.listFiles())
                .filter(f -> f.getName().endsWith(".parquet"))
                .map(f -> "file:///" + f.getAbsolutePath().replace("\\", "/")) // Форматируем пути
                .toArray(String[]::new);

        System.out.println(Arrays.toString(parquetFiles));
        Dataset<Row> df = ss.read().parquet(parquetFiles);
        df.show();

        System.out.println(Arrays.toString(df.columns()));
        System.out.println(df.count());

//        ds.write().option("header", "true")
//                .parquet("rfsd");
//                .csv("partitions");
    }
}
