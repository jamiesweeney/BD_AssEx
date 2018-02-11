package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_v2 extends Configured implements Tool {
		
	static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text, Text, Text> {
		
		private Text word = new Text(); 
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equals("REVISION")) {
					String a = tokenizer.nextToken();
					a = tokenizer.nextToken();
					word.set(tokenizer.nextToken());
				}
				else if (token.equals("MAIN")) {
					while (token.equals("TALK") == false & tokenizer.hasMoreTokens())  {
						token = tokenizer.nextToken();
						Text t = new Text(token);
						context.write(word,t);
					}
				}
			}
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			MapWritable out = new MapWritable();
			out.put(new Text("rank"), new FloatWritable ((float) 1.0));
	        String output = "";

	        for (Text value : values){
	        	output = output.concat(value.toString() + " ");
	        }
	        out.put(new Text("links"), new Text(output));

	        context.write((key),new Text(output));
		}
	}
	
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v1");
		job.setJarByClass(WordCount_v2.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(MyInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_v2(), args));
	}
}
	
	