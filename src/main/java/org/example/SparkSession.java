package org.example;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSession {
    public static Dataset<Row> dtf () {
        String file = "E:\\Downloads\\US_Accidents_Dec21_updated.csv\\US_Accidents_Dec21_updated.csv";
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
                .appName("MyApp")
                .master("local[*]")
                .getOrCreate();


        Dataset<Row> df = spark.read()
                .option("header", true)
                .csv(file);

        df.printSchema();
        //df.explain();
        //df.show(50);

        // Stop the SparkSession
        spark.stop();
        return df;
    }

}
