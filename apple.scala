import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object apple{
 def main(args:Array[String]): Unit = {
 val df = spark.read.option("header",true).csv("/user/fdm-user/stocks/yahoo_daily.csv")
 df.show()
 spark.sql("show databases").show()
 spark.sql("use olympics").show()
 spark.sql("show tables").show()
 spark.sql("select * from athletesdata").show()
 }
}
