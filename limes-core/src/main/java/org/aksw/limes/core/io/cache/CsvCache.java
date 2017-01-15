package org.aksw.limes.core.io.cache;

import org.aksw.limes.core.io.config.KBInfo;
import org.aksw.limes.core.io.preprocessing.Preprocessor;
import org.aksw.limes.core.io.query.IQueryModule;
import org.aksw.limes.core.io.query.QueryModuleFactory;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Cache class for saving data to csv file. For testing with the LIMES-Flink engine.
 *
 * @author Christopher Rost (c.rost@studserv.uni-leipzig.de)
 * @version Nov 12, 2016
 */
public class CsvCache extends ACache implements ICache{

    static Logger logger = LoggerFactory.getLogger(CsvCache.class.getName());
    // maps uris to instance. A bit redundant as instance contain their URI
    protected HashMap<String, Instance> instanceMap;
    //Iterator for getting next instance
    protected Iterator<Instance> instanceIterator;
    // pointing to the parent folder of the "cache" folder
    private File folder = new File("");

    final String CSV_SEPARATOR = "~¿~";


    /**
     * Constructor
     */
    public CsvCache() {
        instanceMap = new HashMap<>();
    }

    /**
     * Create cache specifying the parent folder. Make shure the Application has write permissions there.
     *
     * @param folder
     *         File pointing to the the parent folder of the (to-be-created) "cache" folder.
     */
    public CsvCache(File folder) {
        this();
        this.folder = folder;
    }

    public static CsvCache getData(KBInfo kb) {
        return getData(new File(""), kb);
    }

    public static CsvCache getData(File folder, KBInfo kb) {
        CsvCache cache = new CsvCache(folder);
        // Create hash string for file name
        String hash = kb.hashCode() + "";
        File cacheFile = new File(folder + "cache/" + hash + ".csv");
        logger.info("Checking for file " + cacheFile.getAbsolutePath());
        try {
            if (cacheFile.exists()) {
                logger.info("Found cached data. Loading data from file " + cacheFile.getAbsolutePath());
                cache = CsvCache.loadFromFile(cacheFile);
            } else {
                throw new Exception();
            }
            if (cache.size() == 0) {
                /**
                 * TODO: Uncomment if loadFromFile Method is fully implemented

                throw new Exception();

                 */

            } else {
                logger.info("Cached data loaded successfully from file " + cacheFile.getAbsolutePath());
                logger.info("Size = " + cache.size());
            }
        } catch (Exception e) {

            // need to add a QueryModuleFactory
            logger.info("No cached data found for " + kb.getId());
            IQueryModule module = QueryModuleFactory.getQueryModule(kb.getType(), kb);
            module.fillCache(cache);

            if (!new File(folder.getAbsolutePath() + File.separatorChar + "cache").exists() || !new File(folder.getAbsolutePath() + File.separatorChar + "cache").isDirectory()) {
                new File(folder.getAbsolutePath() + File.separatorChar + "cache").mkdir();
            }
            cache.saveToFile(new File(folder.getAbsolutePath() + File.separatorChar + "cache/" + hash + ".csv"));
        }
        return cache;
    }

    /**
     * TODO: NOT Implemented yet
     *
     * @param file
     * @return
     * @throws IOException
     */
    private static CsvCache loadFromFile(File file) throws IOException {
        String path = file.getAbsolutePath();
        String parentPath = path.substring(0, path.lastIndexOf("cache"));
        File parent = new File(parentPath);

        FileInputStream in = new FileInputStream(file);

        CsvCache cache = new CsvCache();

        //TODO: Not implemented yet.
        return cache;
    }

    /**
     * Save to file
     *
     * @param file the file to save
     */
    private void saveToFile(File file) {
        final String codePage = "UTF-8";
        logger.info("Saving cache to csv file: " + file.getAbsolutePath());
        try {
            // Create a BufferedWriter for csv file
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), codePage));

            //Write csv heading
            // first column in csv is the uri
            bufferedWriter.write("uri");
            bufferedWriter.write(CSV_SEPARATOR);
            // property name is 2nd col in csv ...
            bufferedWriter.write("property");
            bufferedWriter.write(CSV_SEPARATOR);
            // followed by its values as separate col
            bufferedWriter.write("value");
            bufferedWriter.newLine();

            Instance i = getNextInstance();

            // iterate over all instances
            while (i != null) {
                Set<String> props = i.getAllProperties();
                for (String prop : props) {
                    for (String value : i.getProperty(prop)) {
                        // first column in csv is the uri
                        bufferedWriter.write(i.getUri());
                        bufferedWriter.write(CSV_SEPARATOR);
                        // property name is 2nd col in csv ...
                        bufferedWriter.write(prop);
                        bufferedWriter.write(CSV_SEPARATOR);
                        // followed by its values as separate col
                        bufferedWriter.write(value);
                        bufferedWriter.newLine();
                    }
                }
                i = getNextInstance();
            }

            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (FileNotFoundException fileNotFoundException) {
            logger.error("File " + file.getAbsolutePath() + " can not be created.");
            logger.error(this.getClass().getName() + " throws Exception: " + fileNotFoundException.getMessage());
        } catch (UnsupportedEncodingException encodingException) {
            logger.error("Encoding " + codePage + " is not supported.");
            logger.error(this.getClass().getName() + " throws Exception: " + encodingException.getMessage());
        } catch (IOException ioException) {
            logger.error(this.getClass().getName() + " throws Exception: " + ioException.getMessage());
        }
        logger.info("Cache csv file successfully saved as: " + file.getAbsolutePath());
    }

    /**
     * Adds an Instance object to cache.
     *
     * @param i the Instance object to be added
     */
    @Override
    public void addInstance(Instance i) {
        if (!instanceMap.containsKey(i.getUri())) {
            instanceMap.put(i.getUri(), i);
        }
    }

    /**
     * Returns the next instance in the list of instances
     *
     * @return next Instance or null if no next instance
     */
    @Override
    public Instance getNextInstance() {
        if (instanceIterator == null) {
            instanceIterator = instanceMap.values().iterator();
        }

        if (instanceIterator.hasNext()) {
            return instanceIterator.next();
        } else {
            return null;
        }
    }

    /**
     * Returns all the instance contained in the cache
     *
     * @return ArrayList containing all instances of type Instance
     */
    @Override
    public ArrayList<Instance> getAllInstances() {
        return new ArrayList<>(instanceMap.values());
    }

    /**
     * Returns ArrayList of all URIs
     *
     * @return ArrayList of all URIs
     */
    @Override
    public ArrayList<String> getAllUris() {
        return new ArrayList<>(instanceMap.keySet());
    }

    /**
     * Adds a new spo statement to the cache
     *
     * @param s
     *         The URI of the instance linked to o via p
     * @param p
     *         The property which links s and o
     * @param o
     *         The value of the property of p for the entity s
     */
    @Override
    public void addTriple(String s, String p, String o) {
        if (instanceMap.containsKey(s)) {
            Instance m = instanceMap.get(s);
            m.addProperty(p, o);
        } else {
            Instance m = new Instance(s);
            m.addProperty(p, o);
            instanceMap.put(s, m);
        }
    }

    /**
     * Check if Instance exists
     *
     * @param i
     *         The instance to look for
     * @return true if the URI of the instance is found in the cache
     */
    @Override
    public boolean containsInstance(Instance i) {
        return instanceMap.containsKey(i.getUri());
    }

    /**
     * Check if URI exists
     *
     * @param uri
     *         The URI to looks for
     * @return True if an instance with the URI uri is found in the cache, else false
     */
    @Override
    public boolean containsUri(String uri) {
        return instanceMap.containsKey(uri);
    }

    /**
     * Get Instance from cache
     *
     * @param uri
     *         URI to look for
     * @return The instance with the URI uri if it is in the cache, else null
     */
    @Override
    public Instance getInstance(String uri) {
        if (instanceMap.containsKey(uri)) {
            return instanceMap.get(uri);
        } else {
            return null;
        }
    }

    /**
     * Resets the iterator.
     */
    @Override
    public void resetIterator() {
        instanceIterator = instanceMap.values().iterator();
    }

    /**
     * Get the size of the cache.
     * @return size of cache as int
     */
    @Override
    public int size() {
        return instanceMap.size();
    }

    /**
     * Get sample cache with given size
     *
     * @param size as int for the cache sample
     * @return a CsvCache object
     */
    @Override
    public ACache getSample(int size) {
        ACache c = new CsvCache();
        ArrayList<String> uris = getAllUris();
        while (c.size() < size) {
            int index = (int) Math.floor(Math.random() * size());
            Instance i = getInstance(uris.get(index));
            c.addInstance(i);
        }
        return c;
    }

    /**
     * Replaces an Instance
     *
     * @param uri the URI of the Instance to replace as String
     * @param a the Instance object
     */
    @Override
    public void replaceInstance(String uri, Instance a) {
        if (instanceMap.containsKey(uri)) {
            instanceMap.remove(uri);
        }
        instanceMap.put(uri, a);
    }

    /**
     * Fetch all Properties
     * @return Set of Properties
     */
    @Override
    public Set<String> getAllProperties() {
        logger.debug("Get all properties...");
        if (this.size() > 0) {
            HashSet<String> props = new HashSet<>();
            ACache c = this;
            for (Instance i : c.getAllInstances()) {
                props.addAll(i.getAllProperties());
            }
            return props;
        } else {
            return new HashSet<>();
        }
    }

    /**
     * Process data
     * @param propertyMap Map of String,String
     * @return the CsvCache object
     */
    @Override
    public ACache processData(Map<String, String> propertyMap) {
        ACache c = new CsvCache();
        for (Instance instance : getAllInstances()) {
            String uri = instance.getUri();
            for (String p : instance.getAllProperties()) {
                for (String value : instance.getProperty(p)) {
                    if (propertyMap.containsKey(p)) {
                        c.addTriple(uri, p, Preprocessor.process(value, propertyMap.get(p)));
                    } else {
                        c.addTriple(uri, p, value);
                    }
                }
            }
        }
        return c;
    }

    /**
     * @param sourcePropertyName
     *         Name of the property to process.
     * @param targetPropertyName
     *         Name of the new property to process data into.
     * @param processingChain
     *         Preprocessing Expression.
     * @return CsvCache
     */
    @Override
    public ACache addProperty(String sourcePropertyName, String targetPropertyName, String processingChain) {
        ACache c = new CsvCache();
        for (Instance instance : getAllInstances()) {
            String uri = instance.getUri();
            for (String p : instance.getAllProperties()) {
                for (String value : instance.getProperty(p)) {
                    if (p.equals(sourcePropertyName)) {
                        c.addTriple(uri, targetPropertyName, Preprocessor.process(value, processingChain));
                        c.addTriple(uri, p, value);
                    } else {
                        c.addTriple(uri, p, value);
                    }
                }
            }
        }
        logger.debug("Cache is ready");
        return c;
    }

    /**
     * Not implemented yet.
     *
     * @param baseURI
     *         Base URI of properties, could be empty.
     * @param IDbaseURI
     *         Base URI for id of resources: URI(instance) := IDbaseURI+instance.getID(). Could be empty.
     * @param rdfType
     *         rdf:Type of the instances.
     * @return
     */
    @Override
    public Model parseCSVtoRDFModel(String baseURI, String IDbaseURI, String rdfType) {
        logger.debug("Non implemented method parseCSVtoRDFModel was used :( ");
        return null;
    }
}
