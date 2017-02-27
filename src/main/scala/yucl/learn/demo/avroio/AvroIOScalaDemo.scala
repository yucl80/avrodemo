package yucl.learn.demo.avroio

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.hadoop.io.AvroSequenceFile
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic._
import org.apache.avro.mapred.FsInput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import com.databricks.spark.avro._

object AvroIOScalaDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("sss")
      .setMaster("local[*]")
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = Seq(
      (2017, 8, "/test/a.html", 200),
      (2017, 8, "/test/a.htm", 200),
      (2017, 7, "/test/b.htm", 404),
      (2017, 7, "/test/c.htm", 500)).toDF("year", "month", "uri", "code")

    sqlContext.setConf("spark.sql.avro.compression.codec", "deflate")

    val filePath = "/tmp/dfdata12"

    // val name = "AvroTest"
    // val namespace = "yucl.learn.demo"
    // val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)
    // df.write.options(parameters).partitionBy("year", "month").avro("/tmp/a.avro")

    df.write.mode(SaveMode.Append).partitionBy("year", "month").avro(filePath)

    sqlContext.sql("CREATE TEMPORARY TABLE table_name USING com.databricks.spark.avro OPTIONS (path \"" + filePath + "\")")
    val df2 = sqlContext.sql("SELECT * FROM table_name")
    df2.foreach { x => println(x) }

    val df1 = sqlContext.read.avro(filePath)
    df1.printSchema()
    df1.filter("year = 2011").collect().foreach(println)
  }
}