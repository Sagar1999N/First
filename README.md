# HR Analytics with Spark (Java)

## About
Spark-based pipeline to analyze employee dataset:
- Find top earners
- Compute department spend
- Save results in Parquet

## Technologies
- Apache Spark 3.5
- Java 11
- Hadoop 3.x

## How to Run
mvn clean install
java -cp target/spark-hr-1.0-SNAPSHOT.jar com.sagar.spark.HRAnalytics
