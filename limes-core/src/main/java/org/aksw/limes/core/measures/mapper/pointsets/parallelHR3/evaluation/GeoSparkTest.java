package org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.evaluation;

import org.aksw.limes.core.io.mapping.AMapping;
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

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Test GeoHR3, ParallelGeoHR3, GeoSpark
 * @author Khai Nguyen
 */

public class GeoSparkTest{
	
	public static void main(String[] args) throws IOException, URISyntaxException
	{
		JavaSparkContext sc = new JavaSparkContext("local[2]", "limes");
		
	    Logger logger = Logger.getLogger(GeoSparkTest.class.getName());
	    
	 	Set<Polygon> sourcePolygonSet = PolygonReader.readPolygons(args[0]);
		Set<Polygon> targetPolygonSet = PolygonReader.readPolygons(args[1]);	
		
		logger.info("-------------------- Input data --------------------");
		logger.info("Source: " + sourcePolygonSet.size());
		logger.info("Target: " + targetPolygonSet.size());
		
		logger.info("-------------------- GeoHR3 --------------------");
		long startTime = System.currentTimeMillis();
		GeoHR3 geoHr3 = new GeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY, MeasureType.GEO_NAIVE_HAUSDORFF);
		AMapping mapping = geoHr3.run(sourcePolygonSet, targetPolygonSet);
		long hr3Time = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("GeoHR3 total time = " + hr3Time + "ms");

		logger.info("-------------------- Parallel GeoHR3 --------------------");
		int nThreadCount = 2;
		startTime = System.currentTimeMillis();
		ParallelGeoHR3 parallelGeoHR3 = new ParallelGeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY,
				MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(nThreadCount), nThreadCount); 
		logger.info("ParallelGeoHr3 constructor time = " + (System.currentTimeMillis() - startTime) + "ms");
		mapping = parallelGeoHR3.run(sourcePolygonSet, targetPolygonSet);
		long parallelHr3Time = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("ParallelGeoHR3 total time = " + parallelHr3Time + "ms");
		double speadUp = ((double) (hr3Time - parallelHr3Time) / hr3Time) * 100;
		logger.info("Speed up = " + speadUp + "%");
		
		logger.info("-------------------- Spark --------------------");
		startTime = System.currentTimeMillis();
		GeoSpark geoSprk = new GeoSpark(0.5f, GeoHR3.DEFAULT_GRANULARITY, MeasureType.GEO_NAIVE_HAUSDORFF, null, nThreadCount);
		mapping = geoSprk.run(sourcePolygonSet, targetPolygonSet, sc, logger);
		long sparkTime = System.currentTimeMillis() - startTime;
		logger.info("Found correspondences = " + mapping.size());
		logger.info("Spark total time = " + sparkTime + "ms");
		speadUp = ((double) (hr3Time - sparkTime) / hr3Time) * 100;
		logger.info("Speed up = " + speadUp + "%");
	
		sc.close();
	}
	
}
