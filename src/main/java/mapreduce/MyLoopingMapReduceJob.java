package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

import mapreduce.WordCount_v2.Map;
import mapreduce.WordCount_v2.OtherInputFormat;
import mapreduce.WordCount_v2.Reduce;

public class MyLoopingMapReduceJob extends Configured implements Tool {

	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}

		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		@Override
		protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			String entry = value.toString();
			String articleName;
			Text src;
			Text dst;
			StringTokenizer tokenizer = new StringTokenizer(entry);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equals("REVISION")) {
					articleName = tokenizer.nextToken();
					src.set(articleName);
				}
				else if (token.equals("MAIN")) {
					token = tokenizer.nextToken();
					ArrayList<String> links = new ArrayList<String>();
					while (token.equals("TALK") == false)  {
						links.add(token);
					}
				}
			}
			IntWritable totalW = new IntWritable(total);
			context.write(word, totalW);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		
		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// ...
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	// Your main Driver method. Note: everything in this method runs locally at the client.
	public int run(String[] args) throws Exception {
		
		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Job job = Job.getInstance(getConf());

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		job.setJarByClass(MyLoopingMapReduceJob.class);

		// 2. Set mapper and reducer classes
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
			
		// 3. Set final output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		// 4. Get args variables
		String inputPath = args[0];
		String outputPath = args[1];
		int numLoops = Integer.parseInt(args[2]);
		
		boolean succeeded = false;
		for (int i = 0; i < numLoops; i++) {
			// 5. Set input and output format, mapper output key and value classes, and final output key and value classes
			if (i == 0) {
				FileInputFormat.setInputPaths(job, new Path(inputPath));
				FileOutputFormat.setOutputPath(job, new Path(outputPath + "/" + Integer.toString(i)));
			}else { 
				FileInputFormat.setInputPaths(job, new Path(outputPath + "/" + Integer.toString(i-1)));
				FileOutputFormat.setOutputPath(job, new Path(outputPath + "/" + Integer.toString(i)));
			}
			
			job.setInputFormatClass(OtherInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

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
