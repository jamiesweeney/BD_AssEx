package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer class for last iteration
//Same as MyPageRankReducer except it does not print links in output
class MyLastReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	// Main reducing method
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> iter = values.iterator();
		
		// Collect sum of new additions to page rank
		if (iter.hasNext()) {
			float sum = (float) 0;
			
			while (iter.hasNext()) {
				String val = iter.next().toString();
				try {
					sum = sum + (Float.parseFloat(val));
				}catch (NumberFormatException e) {
				}
				
			}
			
			// Calculate new rank and write in format (page, pageRank)
			float newRank = (float) ((float)0.15 + ((float)0.85*sum));
			Text output = new Text(Float.toString(newRank));
			
			context.write(key, output);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
