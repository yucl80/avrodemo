package yucl.learn.demo.avroio;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroIOJavaDemo {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.56.134:9000");
		String filePath = "/tmp/avroIOJavaDemo8.avro";
		writeSpecific(conf, filePath);
		writeReflect(conf, filePath);
		readGeneric(conf,filePath);
		readReflect(conf, filePath);
	}

	public static void writeSpecific(Configuration conf, String filePath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Schema logSchema = new Schema.Parser().parse(Thread.currentThread()
				.getContextClassLoader().getResourceAsStream("logEntity.avsc"));
		SpecificDatumWriter<GenericRecord> sdw = new SpecificDatumWriter<GenericRecord>(
				logSchema);
		DataFileWriter<GenericRecord> dfwo = new DataFileWriter<GenericRecord>(
				sdw);
		Path path = new Path(filePath);
		OutputStream out = null;
		DataFileWriter<GenericRecord> dfw = null;
		if (fs.exists(path)) {
			out = fs.append(path);
			dfw = dfwo.appendTo(new FsInput(path, conf), out);
		} else {
			out = fs.create(path, false);
			dfw = dfwo.create(logSchema, out);
		}
		GenericRecord logEntity1 = new GenericData.Record(logSchema);
		logEntity1.put("uri", "/tmp/index.html");
		logEntity1.put("host", "192.168.0.21");
		logEntity1.put("atime", 1488166606491l);
		dfw.append(logEntity1);
		dfw.close();
		out.close();
	}

	public static void writeReflect(Configuration conf, String filePath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Schema logSchema = ReflectData.get().getSchema(LogEntity.class);
		ReflectDatumWriter<LogEntity> rdw = new ReflectDatumWriter<LogEntity>(
				LogEntity.class);
		DataFileWriter<LogEntity> dfwo = new DataFileWriter<LogEntity>(rdw);
		Path path = new Path(filePath);
		OutputStream out = null;
		DataFileWriter<LogEntity> dfw = null;
		if (fs.exists(path)) {
			out = fs.append(path);
			dfw = dfwo.appendTo(new FsInput(path, conf), out);
		} else {
			out = fs.create(path, false);
			dfw = dfwo.create(logSchema, out);
		}
		dfw.append(new LogEntity("/index.html", "192.168.0.22", 1488166606000l));
		dfw.close();
		out.close();
	}

	public static void readReflect(Configuration conf, String filePath)
			throws IOException {
		Path path = new Path(filePath);
		SeekableInput input = new FsInput(path, conf);
		Schema logSchema = ReflectData.get().getSchema(LogEntity.class);
		ReflectDatumReader<LogEntity> reader = new ReflectDatumReader<LogEntity>(
				logSchema);
		FileReader<LogEntity> fileReader = DataFileReader.openReader(input,
				reader);
		while (fileReader.hasNext()) {
			LogEntity datum = fileReader.next();
			System.out.println("value = " + datum);
		}
		fileReader.close();
	}

	public static void readGeneric(Configuration conf, String filePath)
			throws IOException {
		Path path = new Path(filePath);
		SeekableInput input = new FsInput(path, conf);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		FileReader<GenericRecord> fileReader = DataFileReader.openReader(input,
				reader);
		for (GenericRecord datum : fileReader) {
			System.out.println("value = " + datum);
		}
		fileReader.close();
	}

	public static class LogEntity {
		String uri;
		String host;
		long atime;

		public LogEntity() {

		}

		public LogEntity(String uri, String host, long atime) {
			this.uri = uri;
			this.host = host;
			this.atime = atime;
		}

		public String toString() {
			return "LogEntity [uri=" + uri + ", host=" + host + ", atime="
					+ atime + "]";
		}

	}
}
