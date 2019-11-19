package com.upgrad.trendproject.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.upgrad.trendproject.pojo.TrendPojo;

public class SaavnTrendMapper extends Mapper<Object, Text, TrendPojo, FloatWritable>{
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		

		String stream[] = value.toString().split(",");
		
		//If stream contains extra , or some improper value the don't process
		if(stream.length!=5)
			return;
  		
		//Extract Data
  		String songId = stream[0];
  		String[] date = stream[4].split("-");
  		if(date.length != 3) 
  			return;
  		
	  	int day = Integer.parseInt(date[2]);
	  	int hour = Integer.parseInt(stream[3]); 
	  	
	  	//Window is One day so ignore other days
	  	if(day<24 || day==31)
			return;
		
	  	//Send Data to Combiner
	  	TrendPojo pojo = new TrendPojo();
		pojo.setSongId(new Text(songId));
		pojo.setDate(new IntWritable(day));
		float score = calcScore(hour);
		context.write(pojo, new FloatWritable(score));
		
	}
	
	private float calcScore(int hour) {
		final int r = 23 - hour;
		return (float) Math.pow(0.95, r);
	}
	
}
