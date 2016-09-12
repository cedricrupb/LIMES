package org.aksw.limes.core.measures.mapper.pointsets.parallelHR3;

import scala.Tuple2;

import org.aksw.limes.core.controller.Controller;
import org.aksw.limes.core.io.mapping.AMapping;
import org.aksw.limes.core.io.serializer.ISerializer;
import org.aksw.limes.core.io.serializer.SerializerFactory;
import org.aksw.limes.core.measures.mapper.pointsets.GeoHR3;
import org.aksw.limes.core.measures.mapper.pointsets.Polygon;
import org.aksw.limes.core.measures.mapper.pointsets.PolygonReader;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.ParallelGeoHR3;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.NaiveGeoLoadBalancer;
import org.aksw.limes.core.measures.measure.MeasureType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class GeoSpark {
	public static void main(String[] args) throws IOException
	{
		 args = new String[]{
			"D:/test/nuts_geometry.csv",
			"D:/test/nuts_geometry_.csv"
		 };
		 
    	JavaSparkContext context = new JavaSparkContext("local", "myapp"); 
    	context.addJar("D:/spark-2.0.0-bin-hadoop2.7/local/limes.jar");
    	
		Set<Polygon> sourcePolygonSet = PolygonReader.readPolygons(args[0]);
		Set<Polygon> targetPolygonSet = PolygonReader.readPolygons(args[1]);
		System.out.println("-------------------- Input data --------------------");
		System.out.println("Source: " + sourcePolygonSet.size());
		System.out.println("Target: " + targetPolygonSet.size());

		System.out.println("-------------------- GeoHR3 --------------------");
		long startTime = System.currentTimeMillis();
		GeoHR3 geoHr3 = new GeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY, MeasureType.GEO_NAIVE_HAUSDORFF);
		AMapping mapping = geoHr3.run(sourcePolygonSet, targetPolygonSet);
		long hr3Time = System.currentTimeMillis() - startTime;
		System.out.println("Found correspondences = " + mapping.size());
		System.out.println("GeoHR3 total time = " + hr3Time + "ms");

		System.out.println("-------------------- Parallel GeoHR3 --------------------");
		startTime = System.currentTimeMillis();
		ParallelGeoHR3 parallelGeoHR3 = new ParallelGeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY,
				MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(2), 2); //2 threads
		System.out.println("ParallelGeoHr3 constructor time = " + (System.currentTimeMillis() - startTime) + "ms");
		mapping = parallelGeoHR3.run(sourcePolygonSet, targetPolygonSet);
		long parallelHr3Time = System.currentTimeMillis() - startTime;
		System.out.println("Found correspondences = " + mapping.size());
		System.out.println("ParallelGeoHR3 total time = " + parallelHr3Time + "ms");
		double speadUp = ((double) (hr3Time - parallelHr3Time) / hr3Time) * 100;
		System.out.println("Speed up = " + speadUp + "%");
		
		System.out.println("-------------------- Spark --------------------");
		startTime = System.currentTimeMillis();
		System.out.println("ParallelGeoHr3 constructor time = " + (System.currentTimeMillis() - startTime) + "ms");
		mapping = runSpark(sourcePolygonSet, targetPolygonSet);
		long sparkTime = System.currentTimeMillis() - startTime;
		System.out.println("Found correspondences = " + mapping.size());
		System.out.println("Spark total time = " + sparkTime + "ms");
		speadUp = ((double) (hr3Time - sparkTime) / hr3Time) * 100;
		System.out.println("Speed up = " + speadUp + "%");
		
		
//		 List<String> all = Files.readAllLines(Paths.get(args[0]));
//		 JavaRDD<String> lines = context.parallelize(all);
//		 JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//			 public Iterator<String> call(String s) {
//		 		return  Arrays.asList(s.split(" ")).iterator();
//		 		}
//		 	});	
//		    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
//		    	public Tuple2<String, Integer> call(String s) { 
//		    		return new Tuple2<String, Integer>(s, 1); }
//		    });
//		    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//		    	public Integer call(Integer a, Integer b) {
//		    		return a + b; }
//		    });
//	    
	}
	
	static AMapping runSpark(Set<Polygon> sourcePolygonSet, Set<Polygon> targetPolygonSet)
	{
	//	ParallelGeoHR3 parallelGeoHR3 = new ParallelGeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY,
		//		MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(2), 2); //2 threads
		
		return null;
	}
	
}
