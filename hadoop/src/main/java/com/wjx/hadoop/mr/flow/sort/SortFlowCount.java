package com.wjx.hadoop.mr.flow.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对如下格式的数据进行解析，统计每个用户的上行，下行，总流量,并按总流量降序
 * 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
 * 1363157995052 	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4			4	0	264	0	200
 * @author wjx
 * 2017年5月15日
 */
public class SortFlowCount {
	static class FlowMapper extends Mapper<LongWritable, Text, SortFlowBean,Text>{
		SortFlowBean bean=new SortFlowBean();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String text = value.toString();
			String[] fields = text.split("\t");
			String num=fields[1];
			Long up = Long.valueOf(fields[fields.length-3]);
			Long down = Long.valueOf(fields[fields.length-2]);
			bean.set(up, down);
			context.write(bean, new Text(num));
		}
	}
	
	static class FlowReducer extends Reducer<SortFlowBean,Text, Text, SortFlowBean>{

		@Override
		protected void reduce(SortFlowBean bean, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			context.write(values.iterator().next(),bean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job=Job.getInstance(new Configuration());
		//设置map reduce 类路径
		job.setJarByClass(SortFlowCount.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		//设置mapper 输出k v 类型
		job.setMapOutputKeyClass(SortFlowBean.class);
		job.setMapOutputValueClass(Text.class);
		//设置整个应用输出 k v 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SortFlowBean.class);
		
		Path path = new Path(args[1]);
		FileSystem file = FileSystem.get(new Configuration());
		
		if (file.exists(path)){
			file.delete(path,true);
		}
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, path);
		
		boolean resp = job.waitForCompletion(true);
		
		System.exit(resp?0:1);
	}
}
