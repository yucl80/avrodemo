package yucl.learn.demo.avroio

import java.text.SimpleDateFormat

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, _}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSON

object AccLogSaver {
  val logger: Logger = LoggerFactory.getLogger(AccLogSaver.getClass)

  def main(args: Array[String]) {
    //val List(appName,kafkaZkUrl,topic,kafkaConsumerThreadCount,kafkaStreamCount) = args.toList
    val List(appName, kafkaZkUrl, topic, kafkaConsumerThreadCount, kafkaStreamCount) = List("AccLogSaver", "192.168.21.12,192.168.21.13,192.168.21.14", "parsed-acclog", "6", "1")
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[8]")
    val WINDOW_SIZE = Seconds(10)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, WINDOW_SIZE)
    val kafkaStreams = (1 to kafkaStreamCount.toInt).map { _ =>
      KafkaUtils.createStream(
        streamingContext, kafkaZkUrl, appName, Map(topic -> kafkaConsumerThreadCount.toInt))
    }
    val unifiedStream = streamingContext.union(kafkaStreams)
    val accLogs = unifiedStream
      .map(_._2)
      .map(JSON.parseFull(_))
      .map(_.getOrElse(Map()))
      .map(_.asInstanceOf[Map[String, Any]])
      .filter(_.nonEmpty)
      .filter(_.contains("message"))
      .filter(_.contains("@timestamp"))
      .filter(_.contains("system"))

    accLogs.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        var schema = CachedDataFileWriter.schema
        if (schema == null) {
          val schemaInputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("acclog.avsc")
          CachedDataFileWriter.schema = new Schema.Parser().parse(schemaInputStream)
          schemaInputStream.close()
          schema = CachedDataFileWriter.schema
        }
        val conf = new Configuration()
        val fields = List("system", "instance", "@timestamp", "uri", "query", "time", "bytes", "response", "verb", "path", "sessionid", "auth", "agent", "host", "ip", "clientip", "xforwardedfor", "thread", "uidcookie", "referrer", "message")
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        partition.foreach(log => {
          try {
            val record: GenericRecord = new GenericData.Record(schema)
            val Array(year, month) = log.getOrElse("@timestamp", "").asInstanceOf[String].split("-").slice(0, 2)
            record.put("year", year.toInt)
            record.put("month", month.toInt)
            val time = log.getOrElse("time", 0L) match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => null
            }
            val bytes = log.getOrElse("bytes", 0L) match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => null
            }
            val response = log.getOrElse("response", 200) match {
              case i: Integer => i
              case l: Long => l.toInt
              case _ => null
            }
            record.put(fields.head, log.getOrElse(fields.head, "").asInstanceOf[String])
            record.put(fields(1), log.getOrElse(fields(1), "").asInstanceOf[String])
            record.put("timestamp", simpleDateFormat.parse(log.getOrElse(fields(2), "").asInstanceOf[String]).getTime)
            record.put(fields(3), log.getOrElse(fields(3), "").asInstanceOf[String])
            record.put(fields(4), log.getOrElse(fields(4), "").asInstanceOf[String])
            record.put(fields(5), time)
            record.put(fields(6), bytes)
            record.put(fields(7), response)
            record.put(fields(8), log.getOrElse(fields(8), "").asInstanceOf[String])
            record.put(fields(9), log.getOrElse(fields(9), "").asInstanceOf[String])
            record.put(fields(10), log.getOrElse(fields(10), "").asInstanceOf[String])
            record.put(fields(11), log.getOrElse(fields(11), "").asInstanceOf[String])
            record.put(fields(12), log.getOrElse(fields(12), "").asInstanceOf[String])
            record.put(fields(13), log.getOrElse(fields(13), "").asInstanceOf[String])
            record.put(fields(14), log.getOrElse(fields(14), "").asInstanceOf[String])
            record.put(fields(15), log.getOrElse(fields(15), "").asInstanceOf[String])
            record.put(fields(16), log.getOrElse(fields(16), "").asInstanceOf[String])
            record.put(fields(17), log.getOrElse(fields(17), "").asInstanceOf[String])
            record.put(fields(18), log.getOrElse(fields(18), "").asInstanceOf[String])
            record.put(fields(19), log.getOrElse(fields(19), "").asInstanceOf[String])
            record.put(fields(20), log.getOrElse(fields(20), "").asInstanceOf[String])
            //record.put("uuid", UUID.randomUUID.toString)
            logger.debug(record.toString)
            val partitionKeys = List("year", "month", "system")
            val basePath: String = "/tmp/acclog18"
            CachedDataFileWriter.write(record, partitionKeys, basePath, schema, conf)
          } catch {
            case e: Exception => logger.error(log.toString(), e)
          }
        })
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
