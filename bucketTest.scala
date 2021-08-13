object bucketTest{
  def main(args: Array[String]): Unit = {
    (0 to 200000)
      .map(i => (i, s"master_$i"))
      .toDF("key", "value")
      .write
      .format("json")
      .bucketBy(3, "key")
      .mode("overwrite")
      .saveAsTable("master_json")
       }
}
