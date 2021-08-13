import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test{
def main(args: Array[String]): Unit = {
    spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "0")  
    val tables = spark.catalog.listTables
  }
}
