package com.upgrad.trendproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.upgrad.trendproject.mapper.SaavnTrendMapper;
import com.upgrad.trendproject.partitioner.SaavnTrendPartitioner;
import com.upgrad.trendproject.pojo.TrendPojo;
import com.upgrad.trendproject.reducer.SaavnTrendReducer;
import org.apache.hadoop.fs.Path;


public class SaavnTrend extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new Configuration(), new SaavnTrend(), args);
		System.exit(returnStatus);
	}

	/**
	 * Setting Driver Configurations
	 */
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "Saavn Trending Songs");
		job.setJarByClass(SaavnTrend.class);
		
		//Set Input Path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//Mapper
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(TrendPojo.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setMapperClass(SaavnTrendMapper.class);
		job.setNumReduceTasks(7);
		
		//Combiner
		job.setCombinerClass(SaavnTrendReducer.class);
		
		//Partitioner
		job.setPartitionerClass(SaavnTrendPartitioner.class);
		
		//Reducer
		job.setReducerClass(SaavnTrendReducer.class);
		job.setOutputKeyClass(TrendPojo.class);
		job.setOutputValueClass(FloatWritable.class);
		
		//Set Output Configurations
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return 0;
	}
}
