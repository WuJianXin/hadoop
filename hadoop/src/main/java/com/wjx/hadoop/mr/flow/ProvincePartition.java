package com.wjx.hadoop.mr.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 按key分区，分区从 0 开始
 * @author wjx
 * 2017年5月21日
 */
public class ProvincePartition extends Partitioner<Text, FlowBean>{
	private static HashMap<String,Integer> map=new HashMap<>();
	
	static{
		map.put("136",0);
		map.put("137",1);
		map.put("138",2);
		map.put("139",3);
	}
	
	@Override
	public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
		String prefix = key.toString().substring(0,3);
		Integer partition = map.get(prefix);
		return partition==null ? 4:partition;
	}

}
