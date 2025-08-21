package com.sagar.First;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.hadoop.util.VersionInfo;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		System.out.println("Hello World!");
		System.setProperty("hadoop.home.dir", "C:\\hadoop");
		SparkSession sp = SparkSession.builder().master("local[*]").appName("first_app").getOrCreate();
		System.out.println(sp.version());
		System.out.println(VersionInfo.getVersion());
		System.out.println(sp.sparkContext().appName());
		Dataset<Row> employees = sp.read().option("header", true).csv("./data/input/employees.csv");
		employees = employees.withColumn("salary", functions.col("salary").cast(DataTypes.IntegerType));
		employees.show(10, false);
		employees.printSchema();
		Dataset<Row> RichEmployees = employees.filter(functions.col("salary").gt(5000));
		RichEmployees.show(10, false);
		Dataset<Row> GroupedSalary = employees.groupBy("department").agg(functions.avg("salary").as("avg_salary"));
		GroupedSalary.show(10, false);
		GroupedSalary.write().mode(SaveMode.Overwrite).parquet("./data/out");
		System.out.println("writing complete");
	}
}
