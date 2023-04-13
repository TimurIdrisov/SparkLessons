package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Program {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR); //todo
        Dataset<Row> df1 = SparkSession.dtf();
        df1.explain();

//        String file = "E:\\Downloads\\US_Accidents_Dec21_updated.csv\\US_Accidents_Dec21_updated.csv";
//        SparkSession spark = SparkSession.builder()
//                .appName("MyApp")
//                .master("local[*]")
//                .getOrCreate();
//
//
//        Dataset<Row> df = spark.read()
//                .option("header", true)
//                .csv(file);
//
//        df.printSchema();
////        df.explain();
//        df.show(50);
//
//        // Stop the SparkSession
//        spark.stop();

    }

}

