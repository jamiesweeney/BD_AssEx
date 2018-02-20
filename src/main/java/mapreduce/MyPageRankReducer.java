package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer for main pageRank iterations
//Takes in source page as key, and new additions to pageRank / list of links as values
//Also writes total list of links for future iterations
class MyPageRankReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}
	
	// Main reducing method
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> iter = values.iterator();
		if (iter.hasNext()) {
			float sum = (float) 0;
			String links = "";
			
			// For each value
			while (iter.hasNext()) {
				String val = iter.next().toString();
				try {
					// If float then we add that to the sum
					sum = sum + (Float.parseFloat(val));
				}catch (NumberFormatException e) {
					// Else, must be the list of links
					links = val;
				}
				
			}
			
			// Calcuate new rank and write out, along with list of links
			float newRank = (float) ((float)0.15 + ((float)0.85*sum));
			Text output = new Text(Float.toString(newRank) + " " + links);
			
			context.write(key, output);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}
}
