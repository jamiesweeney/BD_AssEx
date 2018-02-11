package mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutputFormat extends FileOutputFormat<Text, MyWritable> {
	 
	protected static class MyRecordWriter extends RecordWriter<Text, MyWritable> {
 
		private DataOutputStream out;
 
		public MyRecordWriter(DataOutputStream out) throws IOException {
			this.out = out;
		}

		public synchronized void write(Text key, MyWritable value) throws IOException {
			
			out.writeBytes(key.toString());
			out.writeBytes(" ");
			out.writeBytes(value.toString());
			out.writeBytes("\n");
		}
 
		public synchronized void close(TaskAttemptContext job) throws IOException {
			out.close();
		}
 
	}
 
	@Override
	public RecordWriter<Text, MyWritable> getRecordWriter(TaskAttemptContext job) throws IOException {

		// Create output file 
		String fileExt = ".txt";
		Path file = getDefaultWorkFile(job, fileExt);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream outFile = fs.create(file, false);
 
		return new MyRecordWriter(outFile);
 
	}

}
	 