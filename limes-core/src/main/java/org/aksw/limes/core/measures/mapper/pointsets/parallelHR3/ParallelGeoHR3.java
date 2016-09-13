/**
 * 
 */
package org.aksw.limes.core.measures.mapper.pointsets.parallelHR3;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import org.jgap.impl.GreedyCrossover;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.aksw.limes.core.measures.mapper.pointsets.GeoHR3;
import org.aksw.limes.core.measures.mapper.pointsets.GeoIndex;
import org.aksw.limes.core.measures.mapper.pointsets.GeoSquare;
import org.aksw.limes.core.measures.mapper.pointsets.OrchidMapper;
import org.aksw.limes.core.measures.mapper.pointsets.Polygon;
import org.aksw.limes.core.measures.mapper.pointsets.PolygonIndex;
import org.aksw.limes.core.measures.mapper.pointsets.PolygonReader;
import org.aksw.limes.core.measures.mapper.pointsets.PropertyFetcher;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.GeoLoadBalancer;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.NaiveGeoLoadBalancer;
import org.aksw.limes.core.measures.mapper.pointsets.parallelHR3.parallelGeoLoadBalancer.PSOGeoLoadBalancer;
import org.aksw.limes.core.measures.measure.pointsets.hausdorff.CentroidIndexedHausdorffMeasure;
import org.aksw.limes.core.measures.measure.pointsets.hausdorff.IndexedHausdorffMeasure;
import org.aksw.limes.core.measures.measure.pointsets.hausdorff.NaiveHausdorffMeasure;
import org.apache.commons.cli.CommandLine;
import org.aksw.limes.core.measures.measure.MeasureFactory;
import org.aksw.limes.core.measures.measure.MeasureType;
import org.aksw.limes.core.controller.Controller;
import org.aksw.limes.core.controller.ResultMappings;
import org.aksw.limes.core.io.cache.HybridCache;
import org.aksw.limes.core.io.config.Configuration;
import org.aksw.limes.core.io.mapping.AMapping;

/**
 * @author sherif
 *
 */
public class ParallelGeoHR3 extends GeoHR3 implements Serializable{

	public long loadBalancerTime;
	public int maxThreadNr;
	protected GeoLoadBalancer geoLoadBalancer;
	public long tasksCreationTime;

	/**
	 * @param args
	 * @author sherif
	 * @throws IOException
	 */

	/**
	 * @param distanceThreshold
	 * @param granularity
	 * @param hd
	 * @author sherif
	 */
	public ParallelGeoHR3()
	{
		this(0.5f, GeoHR3.DEFAULT_GRANULARITY,MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(2), 2);
			//TODO default thread count
	}
	
	public ParallelGeoHR3(float distanceThreshold, int granularity, MeasureType hd, GeoLoadBalancer geoLoadBalancer,
			int maxThreadsNr) {
		super(distanceThreshold, granularity, hd);
		this.geoLoadBalancer = geoLoadBalancer;
		this.maxThreadNr = maxThreadsNr;
	}

	public GeoIndex assignSquares(Set<Polygon> input) {
		ParallelPolygonGeoIndexer parallelPolygonGeoIndexer = new ParallelPolygonGeoIndexer(input, delta, maxThreadNr);
		parallelPolygonGeoIndexer.run();
		return parallelPolygonGeoIndexer.getIndex();
	}

	/**
	 * Runs GeoHR3 for source and target dataset. Uses the set SetMeasure
	 * implementation. FastHausdorff is used as default
	 *
	 * @param sourceData
	 *            Source polygons
	 * @param targetData
	 *            Target polygons
	 * @return Mapping of polygons
	 */
	public AMapping run(Set<Polygon> sourceData, Set<Polygon> targetData) {
		long begin = System.currentTimeMillis();
		GeoIndex source = assignSquares(sourceData);
		GeoIndex target = assignSquares(targetData);
		long end = System.currentTimeMillis();
		indexingTime = end - begin;
		System.out.println("Parallel Geo-Indexing took: " + indexingTime + " ms");
		if (verbose) {
			System.out.println("Geo-Indexing took: " + indexingTime + " ms");
			System.out.println("|Source squares|= " + source.squares.keySet().size());
			System.out.println("|Target squares|= " + target.squares.keySet().size());
			System.out.println("Distance Threshold = " + distanceThreshold);
			System.out.println("Angular Threshold = " + angularThreshold);
			System.out.println("Parallel index = " + source);
		}

		if (setMeasure instanceof CentroidIndexedHausdorffMeasure) {
			((CentroidIndexedHausdorffMeasure) setMeasure).computeIndexes(sourceData, targetData);
		} else if (setMeasure instanceof IndexedHausdorffMeasure) {
			PolygonIndex targetIndex = new PolygonIndex();
			targetIndex.index(targetData);
			((IndexedHausdorffMeasure) setMeasure).targetIndex = targetIndex;
		}

		// create tasks as pairs of squares to be compared
		Multimap<GeoSquare, GeoSquare> tasks = createTasks(source, target);
		// Multimap<GeoSquare, GeoSquare> tasks = createTasksParallel(source,
		// target);
		// System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%5");
		// for(Entry<GeoSquare, GeoSquare> e : tasks.entries()){
		// System.out.println(e.getKey().size() * e.getValue().size());
		// }

		// Divide tasks equal likely between threads
		begin = System.currentTimeMillis();
		List<GeoHR3Thread> threads = new ArrayList<GeoHR3Thread>();
		List<Multimap<GeoSquare, GeoSquare>> balancedLoad = geoLoadBalancer.getBalancedLoad(tasks);
		loadBalancerTime = System.currentTimeMillis() - begin;
		// System.out.println("(Overhead) " + geoLoadBalancer.getName() + "
		// took: " + loadBalancerTime + " ms");

		// System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		// for (Multimap<GeoSquare, GeoSquare> multimap : balancedLoad) {
		// System.out.println(multimap.size());
		// }

		// Run concurrently
		begin = System.currentTimeMillis();
		GeoHR3Thread.allDone = new Semaphore(0);
		for (Multimap<GeoSquare, GeoSquare> task : balancedLoad) {
			GeoHR3Thread t = new GeoHR3Thread(setMeasure, distanceThreshold, task);
			(new Thread(t)).start();
			threads.add(t);
		}

		// Wait until all threads finished
		try {
			GeoHR3Thread.allDone.acquire(maxThreadNr);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// System.out.println("Whole threads execution took: " +
		// (System.currentTimeMillis() - begin) + " ms");
		return GeoHR3Thread.getMapping();
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @author sherif
	 */
	protected Multimap<GeoSquare, GeoSquare> createTasks(GeoIndex source, GeoIndex target) {
		long begin;
		begin = System.currentTimeMillis();
		Multimap<GeoSquare, GeoSquare> tasks = HashMultimap.create();
		for (Integer latIndex : source.squares.keySet()) {
			for (Integer longIndex : source.squares.get(latIndex).keySet()) {
				GeoSquare g1 = source.getSquare(latIndex, longIndex);
				Set<List<Integer>> squares = getSquaresToCompare(latIndex, longIndex, target);
				for (List<Integer> squareIndex : squares) {
					GeoSquare g2 = target.getSquare(squareIndex.get(0), squareIndex.get(1));
					if (!g1.elements.isEmpty() && !g2.elements.isEmpty()) {
						tasks.put(g1, g2);
					}
				}
			}
		}
		System.out.println("(Overhead) Tasks creation took: " + (System.currentTimeMillis() - begin) + " ms");
		return tasks;
	}

	/**
	 * use multi-threading to create tasks
	 * 
	 * @param source
	 * @param target
	 * @return
	 * @author sherif
	 */
	protected Multimap<GeoSquare, GeoSquare> createTasksParallel(GeoIndex source, GeoIndex target) {
		long begin = System.currentTimeMillis();
		TaskCreatorThread.allDone = new Semaphore(0);

		List<TaskCreatorThread> threads = new ArrayList<TaskCreatorThread>();
		int subsetLength = (int) Math.ceil((double) source.squares.size() / maxThreadNr);
		for (int i = 0; i < source.squares.size(); i += subsetLength) {
			TaskCreatorThread t = new TaskCreatorThread(this.distanceThreshold, this.granularity, null, source, target,
					i, subsetLength);
			(new Thread(t)).start();
			threads.add(t);
		}

		try {
			TaskCreatorThread.allDone.acquire(maxThreadNr);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		tasksCreationTime = System.currentTimeMillis() - begin;
		// System.out.println("(Overhead) Parallel Tasks creation took: " +
		// tasksCreationTime + " ms");
		return TaskCreatorThread.getTasks();
	}

	public static void main(String[] args) throws IOException {

		// CommandLine cl = Controller.parseCommandLine(args);
		// Configuration config = Controller.getConfig(cl);
		// HybridCache sourceCache =
		// HybridCache.getData(config.getSourceInfo());
		// HybridCache targetCache =
		// HybridCache.getData(config.getTargetInfo());
		// List<String> properties =
		// PropertyFetcher.getProperties(config.getMetricExpression(),
		// config.getVerificationThreshold());
		// Set<Polygon> sourcePolygonSet = OrchidMapper.getPolygons(sourceCache,
		// properties.get(0));
		// Set<Polygon> targetPolygonSet = OrchidMapper.getPolygons(targetCache,
		// properties.get(1));

		// args=new String[]{
		// "resources/datasets/nuts/nuts_geometry.csv",
		// "resources/datasets/nuts/nuts_geometry.csv"
		// };
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
		System.out.println("GeoHr3 total time = " + hr3Time + "ms");
		System.out.println("Found correspondences = " + mapping.size());

		for (int threadNr = 2; threadNr <= 4; threadNr *= 2) {
			System.out.println("-------------------- Parallel GeoHR3 --------------------");

			System.out.println();
			System.out.println();
			System.out.println("Number of Threads = " + threadNr);
			System.out.println("Hit Enter to start running ");
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			reader.readLine();

			startTime = System.currentTimeMillis();
			ParallelGeoHR3 parallelGeoHR3 = new ParallelGeoHR3(0.5f, GeoHR3.DEFAULT_GRANULARITY,
					MeasureType.GEO_NAIVE_HAUSDORFF, new NaiveGeoLoadBalancer(threadNr), threadNr);
			System.out.println("ParallelGeoHr3 constructor time = " + (System.currentTimeMillis() - startTime) + "ms");
			mapping = parallelGeoHR3.run(sourcePolygonSet, targetPolygonSet);
			long parallelHr3Time = System.currentTimeMillis() - startTime;
			System.out.println("ParallelGeoHr3 total time = " + parallelHr3Time + "ms");
			System.out.println("Found correspondences = " + mapping.size());
			double speadUp = ((double) (hr3Time - parallelHr3Time) / hr3Time) * 100;
			System.out.println("Speed up = " + speadUp + "%");
		}
	}

}
