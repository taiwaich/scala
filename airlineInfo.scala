import org.apache.spark.sql.types.IntegerType


val formatter = java.text.NumberFormat.getIntegerInstance

val df3 = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://localhost:9000/user/fdm-user/flightdata/newairdata.csv")

val months = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("hdfs://localhost:9000/user/fdm-user/flightdata/L_MONTHS.csv")


df3.createOrReplaceTempView("flight_details")
months.createOrReplaceTempView("months")

//spark.sql("show tables").show()


val totalFlights = spark.sql("select MONTH, ORIGIN, DEST, LATE_AIRCRAFT_DELAY from flight_details").count()

//val totalFlights = spark.sql("select count(*) from flight_details")




val delayFlights = spark.sql("select MONTH, ORIGIN, DEST, LATE_AIRCRAFT_DELAY from flight_details where DEP_DELAY > 60").count()


print(formatter.format(delayFlights.toInt) + " flights delayed by any time\n")

print(formatter.format(totalFlights.toInt) + " total number of flights\n")

val newval = delayFlights.toDouble / totalFlights.toDouble * 100


println(f"$newval%1.4f" + "% of flights delayed 60 minutes")


spark.sql("SELECT ROUND(AVG(dep_delay),2) as `Average Flight Delay` from flight_details").show()



spark.sql("select months.DESCRIPTION ||' '|| DAY_OF_MONTH AS `date of flight`, ORIGIN, DEST, TAIL_NUM, DEP_TIME from flight_details JOIN months ON flight_details.MONTH = months.code where MONTH = 3 and DAY_OF_MONTH = 1 and TAIL_NUM='215NV' order by DEP_TIME").show()

spark.sql("select day_of_week, count(day_of_week) from (select distinct day_of_week, day_of_month, month from flight_details) group by day_of_week").show();
