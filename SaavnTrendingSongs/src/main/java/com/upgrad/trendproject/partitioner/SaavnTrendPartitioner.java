package com.upgrad.trendproject.partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.upgrad.trendproject.pojo.TrendPojo;

public class SaavnTrendPartitioner extends Partitioner<TrendPojo, DoubleWritable>{

	@Override
	public int getPartition(TrendPojo key, DoubleWritable value, int numOfReducers) {
		int day = key.getDate().get();
		return day-24;
	}

}
