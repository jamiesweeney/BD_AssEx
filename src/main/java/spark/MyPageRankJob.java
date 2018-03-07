package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

// Counts all occurrences of article titles.
public class MyPageRankJob {

	public static void main(String[] args) {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank"));
		
		String outdir = args[1];
		int num = Integer.parseInt(args[2]);
		String timel = args[3];

		// Setup file for first proper iteration
		sc.textFile(args[0])
		// Map out records that are within time limits
		.flatMap(s -> {
			
			List<String> recs = Arrays.asList(s.split("\n\n"));
			List<String> nrecs = new ArrayList<String>();
			
			Iterator<String> it =  recs.iterator();
			while (it.hasNext()) {
				String rec = it.next();
				String time = rec.split("\n")[0].split(" ")[5].split("T")[0];
				if (timel.compareTo(time) <= 0) {
					nrecs.add(rec);
				}
			}
			return nrecs;			
		})
		// Take record and produce pair (art_name, [rev_id, rank, links])
		.mapToPair(s -> {
			String name = s.split("\n")[0].split(" ")[3];
			String r_id = s.split("\n")[0].split(" ")[2];
			
			String[] lnks = s.split("\n")[3].split(" ");
			Set<String> links = new HashSet<String>(Arrays.asList(lnks));
			
			String[] data = new String[] {"1.0", r_id , links.toArray().toString()};
		
			return new Tuple2<String, String[]>(name, data);
		})
		// Take pair and produce 1 key for each as form [rev_id, rank, links]
		.reduceByKey((x, y) -> {
			
			if (Integer.parseInt(x[0]) <= Integer.parseInt(y[0])) {
				return x;		
			}else {
				return y;
			}
		})
		// Save to initial output folder
		.saveAsTextFile(outdir + "/0");
		sc.close();
		
		int fnum = 0;
		// Do each iteration of the page rank algorithm
		for (int i = 0; i < num; i++) {
		
			// Use the last iterations output as input
			sc.textFile(outdir + "/" + Integer.toString(i))
			// Map out each record
			.flatMap(s -> Arrays.asList(s.split("\n")))
			
			.flatMap(s -> {
				
				String name = s.split(" ")[0];
				
				String arr = s.split(" ")[1].substring(1, s.split(" ")[1].length()-1);
				String rev = arr.split(", ")[1];
				Float rank = Float.parseFloat(arr.split(", ")[0]);
				String[] links = arr.split(", ")[2].substring(1, arr.split(", ")[2].length()-1).split(", ");
				
				List<List<String>> ans = new ArrayList<List<String>>();
				
				List<String> pdata = new ArrayList<String>();
				List<String> ldata = new ArrayList<String>();
				
				pdata.add(name);
				pdata.add(rev);
				pdata.add(s.split(" ")[1].split(", ")[2]);
				
				ans.add(pdata);
				 
				for (String link : links) {
					ldata.clear();
					ldata.add(link);
					ldata.add(Float.toString((rank/links.length)));					
					ans.add(ldata);
				}
				
				return ans;
			})
			
			
			// Map out 
			.mapToPair(s -> {
				
				if (s.size() == 2) {
					return new Tuple2<String, String[]>(s.get(0), new String[] {s.get(1)});
				}
				return new Tuple2<String, String[]>(s.get(0), new String[] {s.get(1), s.get(2)});
			})
				
			.reduceByKey((x, y) -> {
				
				if (y.length  == 1 && x.length == 1) {
					return new String[] {Float.toString(Float.parseFloat(x[0]) + Float.parseFloat(y[0]))};
				}
				else if (y.length  == 2 && x.length == 1) {
					return new String[] {x[0], y[0], y[1]};
				}
				else if (y.length  == 1 && x.length == 2) {
					return new String[] {y[0], x[0], x[1]};
				}
				else if (y.length == 3 && x.length == 1) {
					Float newr = Float.parseFloat(x[0]) + Float.parseFloat(y[0]);
					return new String[] {Float.toString(newr), y[1], y[2]};
				}
				else if (y.length == 1 && x.length == 3) {
					Float newr = Float.parseFloat(y[0]) + Float.parseFloat(x[0]);
					return new String[] {Float.toString(newr), x[1], x[2]};
				}
				else {
					return new String[] {};
				}
			})
			
			.mapValues(s -> {
					
				if (s.length == 1) {
					Float newr = (float) (0.15 + 0.85*(Float.parseFloat(s[0])));
					return new String[] {Float.toString(newr)};
				}
				else if (s.length == 3) {
					Float newr = (float) (0.15 + 0.85*(Float.parseFloat(s[0])));
					return new String[] {Float.toString(newr), s[1], s[2]};
				}else {
					return new String[] {};
				}
				
			})
			.saveAsTextFile(outdir + "/" + Integer.toString(i+1));
			sc.close();
			fnum = i+1;
		}
		
			
		// Format the final output
		// Use the last iterations output as input
		sc.textFile(outdir + "/" + Integer.toString(fnum))
		// Map out each record
		.flatMap(s -> Arrays.asList(s.split("\n")))
		
		// Map out 
		.mapToPair(s -> {
			
			String name = s.split(" ")[0];
			String arr = s.split(" ")[1].substring(1, s.split(" ")[1].length()-1);
			String rank = arr.split(",")[0];
			
			return new Tuple2<String, String>(name, rank);
		})
		.saveAsTextFile(outdir + "/final");
		sc.close();

	}
}
