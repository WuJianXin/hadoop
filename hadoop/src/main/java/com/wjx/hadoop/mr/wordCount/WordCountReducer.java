package com.wjx.hadoop.mr.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 对每一个单词做次数统计
 * @author wjx
 * 2017年4月30日
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text,LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		long count=0;
		for (LongWritable value:values){
			count+=value.get();
			context.write(key, new LongWritable(count));
		}
	}

}
