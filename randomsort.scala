import org.apache.spark.sql.functions.rand
object randomsort{
 def main(args:Array[String]) = {
    println("How many random numbers to generate? ")
    val aNum=scala.io.StdIn.readInt()
    val r = new scala.util.Random()
    val rdd=sc.parallelize(for (i <- 1 to aNum) yield r.nextInt(10)+1)
    rdd.map(x=>(x,1)).reduceByKey((x,y)=>x+y).collect
    rdd.toDF.show()
    var valsare = rdd.map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(a=>a._2,false).collect.foreach(println)
}
}
