	import org.apache.spark.SparkConf;
	import org.apache.spark.api.java.JavaPairRDD;
	import org.apache.spark.api.java.JavaRDD;
	import org.apache.spark.api.java.JavaSparkContext;
	import org.apache.spark.api.java.function.FlatMapFunction;
	import org.apache.spark.api.java.function.Function;
	import org.apache.spark.api.java.function.PairFunction;
	import org.apache.spark.api.java.*;
	import org.apache.spark.api.java.function.*;
	import scala.Tuple1;
	import scala.Tuple2;
	import java.util.Arrays;
	import java.util.ArrayList;
	import java.util.Collections;
	import java.util.Comparator;
	import java.util.Iterator;
	import java.util.List;
	import java.io.Serializable;
	
	public class SparkSort {
	    
	    public static void main(String[] args) throws Exception {
	        
	        if (args.length != 2)
	        {
	            System.err.println("Usage: SparkSort <in> <out>");
	            System.exit(2);
	        }
	        
	        String inputPath = args[0];
	        
	        SparkConf config = new SparkConf().setAppName("SparkSort").setMaster("yarn");
	        JavaSparkContext sparkContext = new JavaSparkContext(config);
	        
	        JavaRDD<String> lines = sparkContext.textFile(inputPath);
	 
	    
	 
	    
		/*JavaRDD<String> words = lines.flatMap(
	  new FlatMapFunction<String, String>() {
	    @Override public Iterator<String> call(String s) throws Exception{
	      return Arrays.asList(s);
	    }
	  }
	);*/
		JavaRDD<String> words = lines.flatMap(s->Arrays.asList(s).iterator());

	JavaPairRDD<String, String> ones = words.mapToPair(
		new PairFunction<String, String, String>(){
		@Override public Tuple2<String, String> call(String s) throws Exception {
		return new Tuple2(s.substring(0,11),s.substring(11));
		}
		});

	JavaPairRDD<String, String> counts = ones.reduceByKey(
	  new Function2<String, String, String>() {
	   @Override public String call(String s1, String s2) throws Exception{
	      return s1+s2;
	    }
	  }
	);

	JavaPairRDD<String, String> sorted = counts.sortByKey(true);
	for (Tuple2<String, String> tuple : sorted.collect()) {
		System.out.println(tuple._1 + " " + tuple._2+"\r");
		
	}
	   /*JavaRDD<String> out = sorted.flatMap(
	  new FlatMapFunction<String, String>() {
	   @Override public Iterator<String> call(String s) throws Exception{
	      return Arrays.asList(tuple._1 + tuple._2+"\r");
	    }
	  }
	); */
	sorted.flatMap(s->Arrays.asList(s._1+s._2+"\r").iterator()).saveAsTextFile(args[1]);
	    //out.saveAsTextFile(args[1]);
	    
	    System.exit(0); 
	  } 
	  
	
	}
