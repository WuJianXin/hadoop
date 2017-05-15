package com.wjx.hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

//使用更加底层的api调用
//上层那些mapreduce   spark等运算框架，去hdfs中获取数据的时候，就是调的这种底层的api
public class Demo2 {
	FileSystem fs = null;

	@Before
	public void init() throws Exception {

		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");

	}
	
	@Test
	public void testDownLoadFileToLocal() throws IllegalArgumentException, IOException{
		//先获取一个文件的输入流----针对hdfs上的
		FSDataInputStream in = fs.open(new Path("/upload/1.txt"));
		//再构造一个文件的输出流----针对本地的
		FileOutputStream out = new FileOutputStream(new File("f:/2.txt"));
		//再将输入流中数据传输到输出流
		IOUtils.copyBytes(in, out, 4096,true);
	}
	/**
	 * 随机访问
	 * @throws Exception
	 */
	@Test
	public void testRandomAccess() throws Exception{
		FSDataInputStream in = fs.open(new Path("/upload/1.txt"));
		in.seek(2L);
		FileOutputStream out = new FileOutputStream(new File("f:/3.txt"));
		IOUtils.copyBytes(in, out, 4096,true);
	}

	/**
	 * 显示hdfs上文件的内容
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException{
		FSDataInputStream in = fs.open(new Path("/upload/1.txt"));
		IOUtils.copyBytes(in, System.out, 1024);
	}

}
