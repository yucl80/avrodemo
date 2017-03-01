package yucl.learn.demo.avroio

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.FSDataOutputStream

/**
  * Created by yuchunlei on 2017/2/28.
  */
class CachedWriterEntity(var dataFileWriter: DataFileWriter[GenericRecord], var fsDataOutputStream: FSDataOutputStream) {
  var lastWriteTime: Long = 0l

}
