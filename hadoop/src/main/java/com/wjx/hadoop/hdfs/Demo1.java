package com.wjx.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

//使用封装好的api调用
public class Demo1 {
	FileSystem fs=null;
	
	@Before
	public void init(){
		try {
			// 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
			// 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
			// new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
			// 然后再加载classpath下的hdfs-site.xml
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			/**
			 * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
			 */
			conf.set("dfs.replication", "2");

			// 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
			// fs = FileSystem.get(conf);

			// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
			fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
		} catch (Exception e) {
			e.printStackTrace();
		} 

	}
	
	@Test
	public void testAddFileToHdfs() {
		try {
			// 要上传的文件所在的本地路径
			Path src = new Path("f:/LOLCfg.ini");
			// 要上传到hdfs的目标路径
			Path dst = new Path("/upload/LOLCfg.ini");
			fs.copyFromLocalFile(src, dst);
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void copyToLocal(){
		Path src = new Path("/upload/LOLCfg.ini");
		Path dst = new Path("f:/LOLCfg1.ini");
		try {
			fs.copyToLocalFile(false,src, dst,true);
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void cat(){
		Path src = new Path("/upload/LOLCfg.ini");
		try {
			IOUtils.copyBytes(fs.open(src), System.out, 4096, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMkdirAndDeleteAndRename() throws Exception {
		// 创建目录
		fs.mkdirs(new Path("/a/aa"));
		// 删除文件夹 ，如果是非空文件夹，参数2必须给值true
		fs.delete(new Path("/a1"),true);
		// 重命名文件或文件夹
		fs.rename(new Path("/a"), new Path("/a1"));
	}
	
	@Test
	public void testListFiles() throws Exception, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/upload"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getModificationTime());
			System.out.println(file.getOwner());
			System.out.println(file.getPermission());
			System.out.println(file.getReplication());
			System.out.println(file.getBlockSize());
			BlockLocation[] blocks = file.getBlockLocations();
			for(BlockLocation block:blocks){
				System.out.println("block-length:" + block.getLength() + "--" + "block-offset:" + block.getOffset());
				System.out.println(Arrays.toString(block.getHosts()));
				System.out.println(Arrays.toString(block.getNames()));
			}
			System.out.println("--------------------------");
		}
	}
	
	@Test
	public void testListAll() throws Exception{

		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		String flag = "d--             ";
		for (FileStatus fstatus : listStatus) {
			if (fstatus.isFile())  flag = "f--         ";
			System.out.println(flag + fstatus.getPath().getName());
		}
	}

}
