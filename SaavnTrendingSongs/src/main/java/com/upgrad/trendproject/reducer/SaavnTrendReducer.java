package com.upgrad.trendproject.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.upgrad.trendproject.pojo.TrendPojo;

public class SaavnTrendReducer extends Reducer<TrendPojo, FloatWritable, TrendPojo, FloatWritable>{

	@Override
	protected void reduce(TrendPojo key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//Add scores of each date
		float totalScore = 0;
		for(FloatWritable value: values) {
			totalScore += value.get();
		}
		
		//Pass date to next step
		context.write(key, new FloatWritable(totalScore));
	}

}