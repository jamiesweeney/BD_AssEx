package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

class MyFirstReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	// Main reducing method
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Text val = null;
		
		String value;
		String[] parts;
		
		Set<String> links = new HashSet<String>();
		
		int rev = 0;
		int revID;			
		float pageRank = (float) 1.0;
		
		Iterator<Text> iter = values.iterator();
		if (iter.hasNext()) {
		
			value = iter.next().toString();
			parts = value.split(" ");
			revID = Integer.parseInt(parts[0]);
			
			if (revID > rev) {
				val = new Text(value);
				rev = revID;
				links.clear();
				links = new HashSet<String>(Arrays.asList(parts[1].substring(1, parts[1].length()-1).split(",")));
			}
		}
		
		Text output = new Text(Float.toString(pageRank) + " " + links.toString().replaceAll(" ", ""));
		
		context.write(key, output);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}