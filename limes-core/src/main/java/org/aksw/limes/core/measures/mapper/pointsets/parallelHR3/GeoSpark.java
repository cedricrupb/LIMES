package org.aksw.limes.core.measures.mapper.pointsets.parallelHR3;

import java.io.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import org.aksw.limes.core.io.mapping.AMapping;
import org.aksw.limes.core.io.mapping.MappingFactory;
import org.aksw.limes.core.measures.mapper.pointsets.GeoIndex;
import org.aksw.limes.core.measures.mapper.pointsets.GeoSquare;
import org.aksw.limes.core.measures.mapper.pointsets.Polygon;
import org.aksw.limes.core.measures.mapper.pointsets.PolygonIndex;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.GeoLoadBalancer;
import org.aksw.limes.core.measures.measure.MeasureType;
import org.aksw.limes.core.measures.measure.pointsets.IPointsetsMeasure;
import org.aksw.limes.core.measures.measure.pointsets.hausdorff.CentroidIndexedHausdorffMeasure;
import org.aksw.limes.core.measures.measure.pointsets.hausdorff.IndexedHausdorffMeasure;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Spark implementation for GeoHR3
 * 
 * 
 * @author Khai Nguyen
 */

public class GeoSpark extends ParallelGeoHR3 implements Serializable {

	public GeoSpark() {
		super();
	}

	public GeoSpark(float distanceThreshold, int granularity, MeasureType hd, GeoLoadBalancer geoLoadBalancer,
			int maxThreadsNr) {
		super(distanceThreshold, granularity, hd, geoLoadBalancer, maxThreadsNr);
	}

	public AMapping run(Set<Polygon> sourceData, Set<Polygon> targetData, JavaSparkContext sc, Logger logger) {
		
		if (setMeasure instanceof CentroidIndexedHausdorffMeasure) {
			((CentroidIndexedHausdorffMeasure) setMeasure).computeIndexes(sourceData, targetData);
		} else if (setMeasure instanceof IndexedHausdorffMeasure) {
			PolygonIndex targetIndex = new PolygonIndex();
			targetIndex.index(targetData);
			((IndexedHausdorffMeasure) setMeasure).targetIndex = targetIndex;
		}

		// Squares - Squares
		GeoIndex source = assignSquares(sourceData);
		GeoIndex target = assignSquares(targetData);
		JavaRDD<Tuple2<GeoSquare, GeoSquare>> rddSqs = sc.parallelize(toList(createTasksParallel(source, target)));
		
		 // Polygons - Polygons
		JavaRDD<Tuple2<Polygon, Polygon>> rddPlgs = rddSqs
				.flatMap(new FlatMapFunction<Tuple2<GeoSquare, GeoSquare>, Tuple2<Polygon, Polygon>>() {
					public Iterable<Tuple2<Polygon, Polygon>> call(Tuple2<GeoSquare, GeoSquare> t) {
						List<Tuple2<Polygon, Polygon>> p = new ArrayList<Tuple2<Polygon, Polygon>>(
								t._1.elements.size() * t._2.elements.size());
						for (Polygon p1 : t._1.elements)
							for (Polygon p2 : t._2.elements)
								p.add(new Tuple2<Polygon, Polygon>(p1, p2));
						return p;
					}
				});
		//Deduplication		
		JavaPairRDD<String, Tuple2<Polygon, Polygon>> rddClean = rddPlgs
				.mapToPair(new PairFunction<Tuple2<Polygon, Polygon>, String, Tuple2<Polygon, Polygon>>() {
					public Tuple2<String, Tuple2<Polygon, Polygon>> call(Tuple2<Polygon, Polygon> t) {
						return new Tuple2<String, Tuple2<Polygon, Polygon>>(t._1.uri + " - " + t._2.uri, t);
					}
				}).reduceByKey(
						new Function2<Tuple2<Polygon, Polygon>, Tuple2<Polygon, Polygon>, Tuple2<Polygon, Polygon>>() {
							public Tuple2<Polygon, Polygon> call(Tuple2<Polygon, Polygon> v1,
									Tuple2<Polygon, Polygon> v2) {
								return v1;
							}
						});
	
		//Matching 
		JavaRDD<Tuple3<String, String, Double>> rddResult = rddClean
				.map(new Function<Tuple2<String, Tuple2<Polygon, Polygon>>, Tuple3<String, String, Double>>() {
					public Tuple3<String, String, Double> call(Tuple2<String, Tuple2<Polygon, Polygon>> p) {
						return new Tuple3<String, String, Double>(p._2._1.uri, p._2._2.uri,
								setMeasure.computeDistance(p._2._1, p._2._2, 0));
					}
				});
		
		//Thresholding 
		JavaRDD<Tuple3<String, String, Double>> rddFilter = rddResult
				.filter(new Function<Tuple3<String, String, Double>, Boolean>() {
					public Boolean call(Tuple3<String, String, Double> p) {
						return p._3() <= distanceThreshold;
					}
				});
		
		//Collect and output
		AMapping m = MappingFactory.createDefaultMapping();
		List<Tuple3<String, String, Double>> rs = rddFilter.collect();
		for (Tuple3<String, String, Double> r : rs)
			m.add(r._1(), r._2(), 1 / (1 + r._3()));
		
		return m;
	}
	
	List<Tuple2<GeoSquare, GeoSquare>> toList(Multimap<GeoSquare, GeoSquare> index)
	{
		List<Tuple2<GeoSquare, GeoSquare>> lst = new ArrayList<Tuple2<GeoSquare, GeoSquare>>(index.size());
		for(Entry<GeoSquare, GeoSquare> en : index.entries()) 
			lst.add(new Tuple2<GeoSquare, GeoSquare>(en.getKey(), en.getValue()));
		return lst;
	}
}
