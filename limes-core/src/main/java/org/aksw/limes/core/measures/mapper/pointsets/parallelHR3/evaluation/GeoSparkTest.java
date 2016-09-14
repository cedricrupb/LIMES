package org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.evaluation;

import org.aksw.limes.core.io.mapping.AMapping;
import org.aksw.limes.core.io.mapping.MappingFactory;
import org.aksw.limes.core.measures.mapper.pointsets.GeoHR3;
import org.aksw.limes.core.measures.mapper.pointsets.Polygon;
import org.aksw.limes.core.measures.mapper.pointsets.PolygonReader;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.GeoSpark;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.ParallelGeoHR3;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.NaiveGeoLoadBalancer;
import org.aksw.limes.core.measures.measure.MeasureType;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Set;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Test GeoHR3, ParallelGeoHR3, GeoSpark
 * @author Khai Nguyen
 */

public class GeoSparkTest{
	
	public static void main(String[] args) throws IOException, URISyntaxException
	{
		//Run this program:
		//1. export jar with default execution is this class
		//2. run spark-submit
		//./bin/spark-submit.cmd --driver-memory <Memory> <path to jar> <args> 

		String inputSource = args[0];
		String inputTarget = args[1];
		int granularity = Integer.parseInt(args[2]);
		int threadCount = Integer.parseInt(args[3]);
		GeoHR3.DEFAULT_GRANULARITY = granularity; 
		JavaSparkContext sc = new JavaSparkContext("local["+ threadCount + "]", "limes");
		
	    Logger logger = Logger.getLogger(GeoSparkTest.class.getName());
	    logger.addAppender((Appender) new FileAppender(new SimpleLayout(), "./log.txt"));
	    
	 	Set<Polygon> sourcePolygonSet = PolygonReader.readPolygons(inputSource);
		Set<Polygon> targetPolygonSet = PolygonReader.readPolygons(inputTarget);	
		AMapping mapping = MappingFactory.createDefaultMapping();
			
		logger.info("-------------------- Input data --------------------");
		logger.info("Source: " + sourcePolygonSet.size());
		logger.info("Target: " + targetPolygonSet.size());
		
		logger.info("-------------------- GeoHR3 --------------------");
		long startTime = System.currentTimeMillis();
		GeoHR3 geoHr3 = new GeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY, MeasureType.GEO_NAIVE_HAUSDORFF);
		mapping = geoHr3.run(sourcePolygonSet, targetPolygonSet);
		long hr3Time = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("GeoHR3 total time = " + hr3Time + "ms");

		logger.info("-------------------- Parallel GeoHR3 --------------------");
		startTime = System.currentTimeMillis();
		ParallelGeoHR3 parallelGeoHR3 = new ParallelGeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY,
				MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(threadCount), threadCount); 
		mapping = parallelGeoHR3.run(sourcePolygonSet, targetPolygonSet);
		long parallelHr3Time = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("ParallelGeoHR3 total time = " + parallelHr3Time + "ms");
		double speadUp = ((double) (hr3Time - parallelHr3Time) / hr3Time) * 100;
		logger.info("Speed up = " + speadUp + "%");
		
		logger.info("-------------------- Spark --------------------");
		startTime = System.currentTimeMillis();
		GeoSpark geoSprk = new GeoSpark(0.5f, GeoHR3.DEFAULT_GRANULARITY, MeasureType.GEO_NAIVE_HAUSDORFF, null, threadCount);
		mapping = geoSprk.run(sourcePolygonSet, targetPolygonSet, sc, logger);
		long sparkTime = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("Spark total time = " + sparkTime + "ms");
		speadUp = ((double) (hr3Time - sparkTime) / hr3Time) * 100;
		logger.info("Speed up = " + speadUp + "%");
	
		sc.close();
	}
	
}
