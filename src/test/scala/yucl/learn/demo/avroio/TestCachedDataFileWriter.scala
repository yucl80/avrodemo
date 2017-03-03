package yucl.learn.demo.avroio

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration

/**
  * Created by yuchunlei on 2017/2/28.
  */
object TestCachedDataFileWriter {

  def main(args: Array[String]): Unit = {
    val schemaInputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("acclog.avsc")
    val schema = new Schema.Parser().parse(schemaInputStream)
    schemaInputStream.close()
    val conf = new Configuration()

    var a = 0

    // for loop execution with a range
    for (a <- 1 to 10) {

      for (b <- 1 to 10) {

        val record1: GenericRecord = new GenericData.Record(schema)

        record1.put("year", 2017)
        record1.put("month", 3)
        record1.put("system", "LEM")
        record1.put("uri", "/asdfsd-" + a)
        val partitionKeys = List("year", "month")
        val basePath: String = "/tmp/acclog7-" + a
        CachedDataFileWriter.write(record1, partitionKeys, basePath, schema, conf)
        Thread.sleep(10)
        // println(a+"  " +b)
      }
      // Thread.sleep(10)
    }

    //Thread.sleep(5000)
    //System.out.println(record1)
  }

}
