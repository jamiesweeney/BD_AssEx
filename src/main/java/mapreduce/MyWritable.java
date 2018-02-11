package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyWritable implements Writable  {
	
    // Page rank as float and Array list of strings for out-links
    private float rank;
    private ArrayList<String> links;
   
    
    public MyWritable () {
    	this.links = new ArrayList<String>();
    }
    
    public MyWritable (float rank, ArrayList<String> links) throws IOException {
    	MyWritable w = new MyWritable();
        w.readFields(rank, links);
    }
    
    @Override
	public void readFields(DataInput input) throws IOException {
    	float rank = input.readFloat();
    	ArrayList<String> links = new ArrayList<String>();
    	String[] ln = input.readLine().split(" ");
    	for (String link : ln) {
    		links.add(link);
    	}
    	readFields(rank, links);
	}

	public void readFields(float rank, ArrayList<String> links) throws IOException {
    	this.rank = rank;
    	this.links = links;
    }
	
	@Override
	public String toString() {
		
		String output = "";
		output = output.concat(Float.toString(rank));
		
		for (String value : links) {
			output = output.concat(" " + value);
		}
		return output;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBytes(this.toString());
	}

	
  }
