/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.aksw.limes.core.measures.measure.string;

import org.aksw.limes.core.io.cache.Instance;

/**
 * @author Axel-C. Ngonga Ngomo (ngonga@informatik.uni-leipzig.de)
 */
public class LevenshteinMeasure extends AStringMeasure {

    public int getPrefixLength(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int getMidLength(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public double getSizeFilteringThreshold(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public int getAlpha(int xTokensNumber, int yTokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public double getSimilarity(int overlap, int lengthA, int lengthB) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public double getSimilarity(Object object1, Object object2) {
        return (new uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein()).getSimilarity(object1 + "",
                object2 + "");
    }

    public String getName() {
        return "levenshtein";
    }

    public boolean computableViaOverlap() {
        return false;
    }

    // fake value
    public double getRuntimeApproximation(double mappingSize) {
        return mappingSize / 1000d;
    }

}
