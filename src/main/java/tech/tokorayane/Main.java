package tech.tokorayane;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession ss = SparkSession.builder()
                .appName("revenue-computer")
                .getOrCreate();

        ss.sparkContext().setLogLevel("ERROR");

        // Input DataFrame
        StructType schema = DataTypes.createStructType(new StructField[]{
                new StructField("ID", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("NOM", DataTypes.StringType, false, Metadata.empty()),
                new StructField("PRIX", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("VILLE", DataTypes.StringType, false, Metadata.empty()),
                new StructField("DATE", DataTypes.DateType, false, Metadata.empty()),
        });
        Dataset<Row> dfVentes = ss.readStream().option("header", true).schema(schema).csv("hdfs://namenode:8020/data");

        // Data Processing Queries
        Dataset<Row> dfResult = dfVentes.groupBy(col("VILLE")).sum("PRIX");

        // Output
        StreamingQuery query = dfResult
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        query.awaitTermination();
    }
}