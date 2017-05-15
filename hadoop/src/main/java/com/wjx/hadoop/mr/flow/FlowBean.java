package com.wjx.hadoop.mr.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 上行，下行流量bean，实现序列化接口
 * @author wjx
 * 2017年5月15日
 */
public class FlowBean implements Writable{
	private Long up;
	private Long down;
	private Long sum;
	
	/**
	 * 反序列化
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		up=in.readLong();
		down=in.readLong();
	}

	/**
	 * 序列化
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(up);
		out.writeLong(down);
	}
	
	public FlowBean() {
		super();
	}

	public FlowBean(Long up, Long down) {
		super();
		this.up = up;
		this.down = down;
		this.sum = up+down;
	}

	public Long getUp() {
		return up;
	}

	public void setUp(Long up) {
		this.up = up;
	}

	public Long getDown() {
		return down;
	}

	public void setDown(Long down) {
		this.down = down;
	}

	public Long getSum() {
		return sum;
	}

	public void setSum(Long sum) {
		this.sum = sum;
	}
	

	@Override
	public String toString() {
		return "FlowBean [up=" + up + ", down=" + down + ", sum=" + sum + "]";
	}
}
