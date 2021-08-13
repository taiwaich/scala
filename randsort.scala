import org.apache.spark.sql.functions.rand
println("How many random numbers to generate? ")
val aNum=scala.io.StdIn.readInt()

println(aNum)
val r = new scala.util.Random()
val rdd=sc.parallelize(for (i <- 1 to aNum) yield r.nextInt(10)+1)
rdd.toDF.show()
println("numbers generated")
var valsare = rdd.map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(a=>a._2,true).collect.foreach(println)
