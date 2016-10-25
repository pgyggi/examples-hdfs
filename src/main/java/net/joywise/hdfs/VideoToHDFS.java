package net.joywise.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VideoToHDFS {
	public static void main(String args[]) throws Exception {
		Long start = System.currentTimeMillis();
		Configuration conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));
		conf.addResource(new Path("hdfs-site.xml"));
		FileSystem hdfs = FileSystem.get(conf);
		Path mkdir = new Path("/user/longge/radio");
		hdfs.mkdirs(mkdir);
		File f1 = new File("E:\\迅雷下载\\釜山行.HD1280高清韩语特效中字.mp4");
		Path dst = new Path("/user/media/video" + "/" + f1.getName());
		System.out.println(dst.toString());
		hdfs.deleteOnExit(dst);
		InputStream in = null;
		FSDataOutputStream outputStream;
		in = new BufferedInputStream(new FileInputStream(f1));
		byte[] buffer = new byte[256];
		outputStream = hdfs.create(dst);
		int bufferLength;
		while ((bufferLength = in.read(buffer)) > 0)
			outputStream.write(buffer, 0, bufferLength);
		outputStream.close();
		in.close();
		System.out.println((System.currentTimeMillis()-start)/1000);
	}
}
