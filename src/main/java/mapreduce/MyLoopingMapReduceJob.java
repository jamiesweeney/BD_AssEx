package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

// Performs a looped mapreduce job to calculate the pagerank of pages in wikipedia edit history
public class MyLoopingMapReduceJob extends Configured implements Tool {

	// Driver method - runs the job
	public int run(String[] args) throws Exception {
		
		// Get args variables
		String inputPath = args[0];
		String outputPath = args[1];
		int numLoops = Integer.parseInt(args[2]);		
				
		// For each job 
		boolean succeeded = false;
		for (int i = 0; i <= numLoops; i++) {
				
			// Instantiate a Job object
			// We must do this each time to avoid previous job interfering
			Job job = Job.getInstance(getConf());

			// Set the jar name in the job's conf
			job.setJarByClass(MyLoopingMapReduceJob.class);
			
			// Set final output key and value classes
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			// First job, gather pages and out-links
			if (i == 0) {
				job.setMapperClass(MyFirstMapper.class);
				job.setReducerClass(MyFirstReducer.class);
				FileInputFormat.setInputPaths(job, new Path(inputPath));
				job.setInputFormatClass(MyInputFormat.class);	
			// Else, perform another iteration of pagerank algorithm
			}else if (i < numLoops) {
				job.setMapperClass(MyPageRankMapper.class);
				job.setReducerClass(MyPageRankReducer.class);
				FileInputFormat.setInputPaths(job, new Path(outputPath + "/" + Integer.toString(i-1)));
				job.setInputFormatClass(TextInputFormat.class);
			// Else, perform last iteration, same as above but we don't print the links
			}else {
				job.setMapperClass(MyPageRankMapper.class);
				job.setReducerClass(MyLastReducer.class);
				FileInputFormat.setInputPaths(job, new Path(outputPath + "/" + Integer.toString(i-1)));
				job.setInputFormatClass(TextInputFormat.class);
			}
			
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/" + Integer.toString(i)));

			// Submit job and wait - true means give progress.
			succeeded = job.waitForCompletion(true);
			if (!succeeded) {
				break;
			}			
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
	}
}
