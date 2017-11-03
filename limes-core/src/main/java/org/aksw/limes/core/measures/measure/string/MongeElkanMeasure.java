package org.aksw.limes.core.measures.measure.string;

import org.aksw.limes.core.io.cache.Instance;

public class MongeElkanMeasure extends AStringMeasure {

    public static final String split = " ";

    public double proximity(String s1, String s2) {
        String[] sourceToken = s1.split(split);
        String[] targetToken = s2.split(split);
        double simB = 0d;
        double result = 0d;
        TrigramMeasure internMeasure = new TrigramMeasure();
        for (String sourceString : sourceToken) {
            double maxSim = 0d;
            for (String targetString : targetToken) {
                double sim = internMeasure.getSimilarity(sourceString, targetString);
                if (maxSim < sim) {
                    maxSim = sim;
                }
                if (maxSim == 1d) {
                    break;
                }
            }
            simB += maxSim;
        }
        if (simB != 0) {
            result = simB / sourceToken.length;
        }
        return result;
    }

    @Override
    public int getPrefixLength(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getMidLength(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double getSizeFilteringThreshold(int tokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getAlpha(int xTokensNumber, int yTokensNumber, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double getSimilarity(int overlap, int lengthA, int lengthB) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean computableViaOverlap() {
        return false;
    }

    @Override
    public double getSimilarity(Object object1, Object object2) {
        return proximity(object1.toString(), object2.toString());
    }

    @Override
    public String getName() {
        return "ratcliff-obershelp";
    }

    @Override
    public double getRuntimeApproximation(double mappingSize) {
        return mappingSize / 1000d;
    }

}