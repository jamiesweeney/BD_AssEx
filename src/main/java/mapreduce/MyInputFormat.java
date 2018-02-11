package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import mapreduce.MyRecordReader;

public class MyInputFormat extends FileInputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new MyRecordReader();
	}
	
}