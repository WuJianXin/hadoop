package com.wjx.hadoop.mr.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 用空格分割单词
 * @author wjx
 * 2017年4月30日
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		String[] arr = str.split(" ");
		for (String s:arr){
			context.write(new Text(s), new LongWritable(1));
		}
	}
	
}
