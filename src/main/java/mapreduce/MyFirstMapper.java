package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

class MyFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Read record for rev_id, article_name, and list of out-links
		String record = value.toString();
		String[] lines = record.split("\n");
		ArrayList<String> lineArray = new ArrayList<String>(Arrays.asList(lines));			
		
		String revisionLine = lineArray.get(0);
		String revID = revisionLine.split(" ")[2];
		String artName = revisionLine.split(" ")[3];
		
		String mainLine = lineArray.get(3);
		String[] links = mainLine.split(" ");
		Set<String> linksSet = new HashSet<String>();
	
		String outLinks = "";
		
		Boolean first = true;
		for (String link: links) {
			if (first) {
				first = false;
			}else {
				linksSet.add(link);
			}
		}
		
		outLinks = linksSet.toString().replaceAll(" ", "");
		String output = revID + " " + outLinks;
		
		context.write(new Text(artName), new Text(output));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
