package net.joywise.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class MediaToSeq {
    public static void main(String args[]) throws Exception {

        Configuration confHadoop = new Configuration();     
        confHadoop.addResource(new Path("core-site.xml"));
        confHadoop.addResource(new Path("hdfs-site.xml"));   
        FileSystem fs = FileSystem.get(confHadoop);
//        Path inPath = new Path("/mapin/76.jpg");
        File f1 = new File("F:/hbase/1.mp4");
        File f2 = new File("F:/hbase/2.mp4");
//        File f1 = new File("/home/libh/Downloads/1.mp3");
//        File f2 = new File("/home/libh/Downloads/2.mp3");
//        File f1 = new File("/home/libh/Downloads/77.jpg");
//        File f2 = new File("/home/libh/Downloads/76.jpg");
//        Path outPath = new Path("/mapin/mp3.seq");
//        Path outPath = new Path("/mapin/images.seq");
        Path outPath = new Path("/mapin/mp4.seq");
        List<File> files = new ArrayList<File>();
        files.add(f1);files.add(f2);
        InputStream in = null;
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;
        writer = SequenceFile.createWriter(fs, confHadoop, outPath, key.getClass(), value.getClass());
        SequenceFile.Writer.compression(CompressionType.RECORD);
		try {
			for (File f : files) {
				System.out.println("deal with file:"+f.getName());
				in = new BufferedInputStream(new FileInputStream(f));
				byte buffer[] = new byte[in.available()];
				in.read(buffer);
				BytesWritable valueByte = new BytesWritable(buffer);
				writer.append(new Text(f.getName()), valueByte);
			}
        }catch (Exception e) {
            System.out.println("Exception MESSAGES = "+e.getMessage());
        }
        finally {
            IOUtils.closeStream(writer);
            System.out.println("last line of the code....!!!!!!!!!!");
        }
    }
}
