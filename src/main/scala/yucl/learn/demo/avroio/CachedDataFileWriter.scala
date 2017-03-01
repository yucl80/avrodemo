package yucl.learn.demo.avroio

import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic._
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.concurrent.TrieMap

object CachedDataFileWriter {
  val logger: Logger = LoggerFactory.getLogger(CachedDataFileWriter.getClass)
  val fileName: String = UUID.randomUUID().toString
  private val fileCache: TrieMap[String, CachedWriterEntity] = new TrieMap[String, CachedWriterEntity]
  // val pool: ExecutorService = Executors.newFixedThreadPool(1)
  var schema: Schema = null

  def write(record: GenericRecord, partitionKeys: List[String], basePath: String, schema: Schema, configuration: Configuration): Unit = {
    val fileFullName = buildFilePath(record, partitionKeys, basePath) + "/" + fileName + "-" + Thread.currentThread().getId + ".avro"
    val cacheWriterEntity = getDataFileWriter(fileFullName, schema, configuration)
    val dataFileWriter = cacheWriterEntity.dataFileWriter
    dataFileWriter.append(record)
    // dataFileWriter.flush()
    cacheWriterEntity.lastWriteTime = System.currentTimeMillis()

  }

  def buildFilePath(record: GenericRecord, partitionKeys: List[String], basePath: String): String = {
    var filePath: String = ""
    partitionKeys.foreach(pk =>
      filePath = filePath + "/" + pk + "=" + record.get(pk)
    )
    basePath + filePath
  }

  def getDataFileWriter(fileName: String, schema: Schema, conf: Configuration): CachedWriterEntity = {
    fileCache.synchronized {
      var cacheWriterEntity: CachedWriterEntity = fileCache.getOrElse(fileName, null)
      var dfw: DataFileWriter[GenericRecord] = null
      if (cacheWriterEntity == null) {
        val filePath = new Path(fileName)
        val fileSystem = filePath.getFileSystem(conf)
        val datumWriter = new SpecificDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec())
        //dataFileWriter.setCodec(CodecFactory.deflateCodec(5))
        var fsDataOutputStream: FSDataOutputStream = null
        if (fileSystem.exists(filePath)) {
          fsDataOutputStream = fileSystem.append(filePath)
          dfw = dataFileWriter.appendTo(new FsInput(filePath, conf), fsDataOutputStream)
        } else {
          fsDataOutputStream = fileSystem.create(filePath, false)
          dfw = dataFileWriter.create(schema, fsDataOutputStream)
        }

        dfw.setFlushOnEveryBlock(true)
        dfw.setSyncInterval(2048)
        cacheWriterEntity = new CachedWriterEntity(dfw, fsDataOutputStream)
        fileCache.put(fileName, cacheWriterEntity)
      }
      cacheWriterEntity
    }
  }


  def closeTimeoutFiles(): Unit = {
    for ((fileName, writer) <- fileCache) {
      if (System.currentTimeMillis() - writer.lastWriteTime > 1000l) {
        fileCache.remove(fileName)
        writer.dataFileWriter.close()
        writer.fsDataOutputStream.close()
        logger.info(fileName + " remove from writer cache")
      }
    }
  }

  def closeAllFiles(): Unit = {
    for ((fileName, writer) <- fileCache) {
      try {
        fileCache.remove(fileName)
        writer.fsDataOutputStream.close()
        writer.dataFileWriter.close()
        logger.info(fileName + " remove from writer cache")
      }
    }
  }

  val thread = new Thread(new Runnable {
    override def run() = {
      while (true) {
        try {
          Thread.sleep(30000l)
          closeTimeoutFiles()
        }
      }
    }
  })
  thread.setDaemon(true)
  thread.start()

  sys.addShutdownHook(
    closeAllFiles()
  )


}