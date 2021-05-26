import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Main
{
  public static void main(String[] args) {

    SparkSession spark = SparkSession
        .builder()
        .master("local")
        .appName("Near places")
        .getOrCreate();

    Dataset<Row> dataFrame = spark.read().parquet("/Users/ajaymaurya/Downloads/spark/places.parquet");

    Dataset<Row> filteredData = dataFrame.filter(dataFrame.col("devCarrier").isNotNull());

    long noOfOperators=filteredData.select("devCarrier").distinct().count();
    System.out.println("Number of Operators :"+noOfOperators);

    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView("placesData");

    // number of mobile devices used under different operators
    Dataset<Row> data=spark.sql("select devCarrier,count(distinct ifa) AS numberOfMobileDevices from placesData group by devCarrier order by numberOfMobileDevices desc");

    data.show();

    data.write()
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .format("csv")
        .save("/Users/ajaymaurya/Downloads/spark/sparkTaskJava/");

    System.out.println("Data Saved successfully");

  }
}