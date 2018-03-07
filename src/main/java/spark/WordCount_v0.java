package spark;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount_v0 {

	public static void main(String[] args) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		JavaSparkContext sc;
		sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0").setMaster("local[2]").set("spark.executor.memory","1g"));
		sc.textFile(args[0])
		.flatMap(s -> Arrays.asList(s.split(" ")))
		.mapToPair(s -> new Tuple2<String, Integer>(s, 1))
		.reduceByKey((x, y) -> x + y)
		.saveAsTextFile(args[1]);
		sc.close();
	}
}
