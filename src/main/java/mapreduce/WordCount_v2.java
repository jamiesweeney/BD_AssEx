package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

	static class MyRecordReader extends RecordReader<LongWritable, Text> {
		private final byte[] recordSeparator = "\n\n".getBytes();
		private FSDataInputStream fsin;
		private long start, end;
		private boolean stillInChunk = true;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
		throws IOException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(conf);
	
			fsin = fs.open(path);
			start = split.getStart();
			end = split.getStart() + split.getLength();
			fsin.seek(start);
			
			if (start != 0)
			readRecord(false);
		}
		
		
		private boolean readRecord(boolean withinBlock) throws IOException {
			int i = 0, b;
			while (true) {
				if ((b = fsin.read()) == -1)
					return false;
				if (withinBlock)
					buffer.write(b);
				if (b == recordSeparator[i]) {
					if (++i == recordSeparator.length)
						return fsin.getPos() < end;
				} else
					i = 0;
			}
		}
	
		
		public boolean nextKeyValue() throws IOException {
			if (!stillInChunk)
				return false;
			boolean status = readRecord(true);
			value = new Text();
			value.set(buffer.getData(), 0, buffer.getLength());
			key.set(fsin.getPos());
			buffer.reset();
			if (!status)
				stillInChunk = false;
			return true;
		}

		
		public LongWritable getCurrentKey() { return key; }
		
		
		public Text getCurrentValue() { return value; }

		
		public float getProgress() throws IOException {
				return (float) (fsin.getPos() - start) / (end - start);
		}
		
		
		public void close() throws IOException { fsin.close(); }
	}
		

	class MyInputFormat extends FileInputFormat<LongWritable, Text> {
		
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,TaskAttemptContext context) {
			return new MyRecordReader();
		}
	}
	
	static class OtherInputFormat extends FileInputFormat<LongWritable, Text> {
wd
		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
			return new MyRecordReader();
		}
		
	}

		
	static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); 
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int total = 0;
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.equals("REVISION")) {
					word.set(tokenizer.nextToken());
					//context.write(word, one);
				}
				else if (token.equals("MAIN")) {
					token = tokenizer.nextToken();
					total = 0;
					while (token.equals("TALK") == false)  {
						token = tokenizer.nextToken();
						total = total + 1;
					}
				}
			}
			IntWritable totalW = new IntWritable(total);
			context.write(word, totalW);
		}
	}
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value: values)
				sum += value.get();
				context.write(key, new IntWritable(sum));
		}
	}
	
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v1");
		job.setJarByClass(WordCount_v2.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(OtherInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_v2(), args));
	}
}
	
	