package yucl.learn.demo.avroio

import com.databricks.spark.avro._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yuchunlei on 2017/3/3.
  */
object AvroToParquetJob {

  def main(args: Array[String]): Unit = {
    val List(appName: String, srcDir: String, targetDir: String) = List("AvroToParquetJob", "/tmp/acclog-avro", "/tmp/acclog-parquet")
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.sql.avro.compression.codec", "snappy")
    //.set("spark.sql.avro.compression.codec", "deflate")
    // .set("spark.sql.avro.deflate.level", "5")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.avro(srcDir)
    df.repartition(1).write.partitionBy("year", "month", "system").parquet(targetDir)
  }

}
