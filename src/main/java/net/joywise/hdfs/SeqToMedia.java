package net.joywise.hdfs;

import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class SeqToMedia {
	public static void main(String args[]) throws Exception {

		Configuration confHadoop = new Configuration();
		confHadoop.addResource(new Path("core-site.xml"));
		confHadoop.addResource(new Path("hdfs-site.xml"));
		// Path outPath = new Path("/mapin/images.seq");
		// Path outPath = new Path("/mapin/mp3.seq");
		Path outPath = new Path("/mapin/mp4.seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(confHadoop, Reader.file(outPath));
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		while (reader.next(key, value)) {
			value.setCapacity(value.getLength());
			FileOutputStream fos = new FileOutputStream("./" + key);
			fos.write(value.getBytes());
			fos.close();
		}
		IOUtils.closeStream(reader);// 关闭read流

	}
}
