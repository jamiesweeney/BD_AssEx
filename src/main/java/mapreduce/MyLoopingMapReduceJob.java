package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MyLoopingMapReduceJob extends Configured implements Tool {

	// Mapper class for first iteration
	static class MyFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Text artN = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equals("REVISION")) {
					String a = tokenizer.nextToken();
					a = tokenizer.nextToken();
					artN.set(tokenizer.nextToken());
				}
				else if (token.equals("MAIN")) {
					while (token.equals("TALK") == false & tokenizer.hasMoreTokens())  {
						token = tokenizer.nextToken();
						Text t = new Text(token);
						context.write(artN,t);
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	// Reducer class for first iteration
	static class MyFirstReducer extends Reducer<Text, Text, Text, MyWritable> {
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		
		// Main reducing method
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> iter = values.iterator();
			if (iter.hasNext()) {
			
				ArrayList<String> links = new ArrayList<String>();
				float rank = (float) 1.0;
				
				while (iter.hasNext()) {
					links.add(iter.next().toString());
				}
				
				
				
				MyWritable out = new MyWritable();
				out.readFields(rank, links);
				context.write(key,out);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	// Driver method - runs the job
	public int run(String[] args) throws Exception {
		
		// Instantiate a Job object
		Job job = Job.getInstance(getConf());

		// Set the jar name in the job's conf
		job.setJarByClass(MyLoopingMapReduceJob.class);

		// Set mapper and reducer classes
		job.setMapperClass(MyFirstMapper.class);
		//job.setCombinerClass(MyFirstReducer.class);
		job.setReducerClass(MyFirstReducer.class);
			
		// Set final output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
	
	
		// Get args variables
		String inputPath = args[0];
		String outputPath = args[1];
		int numLoops = Integer.parseInt(args[2]);
		
		// For each job we want to do
		boolean succeeded = false;
		for (int i = 0; i < numLoops; i++) {
			
			// Set input and output format, mapper output key and value classes, and final output key and value classes
			FileInputFormat.setInputPaths(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/" + Integer.toString(i)));
			job.setInputFormatClass(MyInputFormat.class);
			job.setOutputFormatClass(MyOutputFormat.class);

			// 7. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
			succeeded = job.waitForCompletion(true);

			if (!succeeded) {
				// 8. The program encountered an error before completing the loop; report it and/or take appropriate action
				// ...
				break;
			}
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
	}
}
