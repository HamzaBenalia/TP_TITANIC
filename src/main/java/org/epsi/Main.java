package org.epsi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void rdd() {

        SparkSession spark = SparkSession.builder()
                .appName("CSV Reader")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/titanic_train.csv");

        csvData.show();
        spark.stop();
    }

    public static void main(String[] args) {
        rdd();
    }
}