package spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

// Counts all occurrences of article titles.
public class WordCount_v2 {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0"));
		sc.textFile(args[0])
		.flatMap(s -> Arrays.asList(s.split("\n\n")))
		.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[3] , s.split("\n")[3].split(" ").length))
		.reduceByKey((x, y) -> x + y)
		.saveAsTextFile(args[1]);
		sc.close();
	}
}
