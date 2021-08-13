object mapTest{
  def main(args: Array[String]): Unit = {
   val transaction = (0 to 2000)
    .map(i => (i, s"transaction_$i"))
    .toDF("key", "value")
    .repartition(3, 'key)
    .cache()

        transaction.count()
       }
}
