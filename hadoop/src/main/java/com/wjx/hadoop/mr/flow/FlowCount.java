package com.wjx.hadoop.mr.flow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对如下格式的数据进行解析，统计每个用户的上行，下行，总流量
 * 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
 * 1363157995052 	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4			4	0	264	0	200
 * @author wjx
 * 2017年5月15日
 */
public class FlowCount {
	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String text = value.toString();
			String[] fields = text.split("\t");
			String num=fields[1];
			Long up = Long.valueOf(fields[fields.length-3]);
			Long down = Long.valueOf(fields[fields.length-2]);
			context.write(new Text(num), new FlowBean(up,down));
		}
	}
	
	static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {
			Long upSum=0L,downSum=0L;
			Iterator<FlowBean> iterator = values.iterator();
			while(iterator.hasNext()){
				FlowBean fb = iterator.next();
				upSum+=fb.getUp();
				downSum+=fb.getDown();
			}
			context.write(key, new FlowBean(upSum,downSum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job=Job.getInstance(new Configuration());
		//设置map reduce 类路径
		job.setJarByClass(FlowCount.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		//设置mapper 输出k v 类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//设置整个应用输出 k v 类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean resp = job.waitForCompletion(true);
		
		System.exit(resp?0:1);
	}
}
