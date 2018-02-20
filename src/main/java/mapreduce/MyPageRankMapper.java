package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class MyPageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split("\t");			
		String source = parts[0];
		String[] words = parts[1].split(" ");
		
		Float pageRank = Float.parseFloat(words[0]);
		if (words.length > 1) {
			String linkT = words[1].substring(1, words[1].length()-1);
			context.write(new Text(source), new Text(words[1]));
			ArrayList<String> links = new ArrayList<String>(Arrays.asList(linkT.split(",")));
			
			Float writeVal = (pageRank / ((float) links.size()));
			
			for (String link : links) {
				context.write(new Text(link), new Text(writeVal.toString()));
			}
		}else {
			context.write(new Text(source), new Text(""));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
