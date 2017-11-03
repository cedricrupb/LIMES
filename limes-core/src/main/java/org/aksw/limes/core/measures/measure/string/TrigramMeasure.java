/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.aksw.limes.core.measures.measure.string;

import java.util.TreeSet;

import org.aksw.limes.core.io.cache.Instance;

/**
 * @author Axel-C. Ngonga Ngomo (ngonga@informatik.uni-leipzig.de)
 */
public class TrigramMeasure extends AStringMeasure {

    public double getSimilarity(int overlap, int lengthA, int lengthB) {
        return ((double) 2 * overlap) / (double) (lengthA + lengthB);
    }

    public String getName() {
        return "Trigram";
    }

    public double getSimilarity(Object object1, Object object2) {
        String p1 = "  " + object1 + "  ";
        String p2 = "  " + object2 + "  ";

        if (p1.length() == 4 && p2.length() == 4)
            return 1.0;
        if ((p1.length() == 4 && p2.length() > 4) || (p2.length() == 4 && p1.length() > 4))
            return 0.0;
        TreeSet<String> t1 = getTrigrams(p1);
        TreeSet<String> t2 = getTrigrams(p2);
        double counter = 0;
        for (String s : t1) {
            if (t2.contains(s))
                counter++;
        }
        return 2 * counter / (t1.size() + t2.size());
    }

    public TreeSet<String> getTrigrams(String a) {
        TreeSet<String> result = new TreeSet<String>();
        String copy = a;

        for (int i = 2; i < copy.length(); i++) {
            result.add(copy.substring(i - 2, i));
        }
        return result;
    }

    public int getPrefixLength(int tokensNumber, double threshold) {
        int k = 1;
        if (threshold == 0)
            k = 0;

        return (tokensNumber - (int) Math.ceil((float) (tokensNumber * threshold / (2 - threshold))) + k);
    }

    public int getMidLength(int tokensNumber, double threshold) {
        int k = 1;
        if (threshold == 0)
            k = 0;

        return (tokensNumber - (int) Math.ceil((float) (tokensNumber * threshold)) + k);
    }

    public double getSizeFilteringThreshold(int tokensNumber, double threshold) {
        return (double) (threshold / (2 - threshold) * tokensNumber);
    }

    public int getAlpha(int xTokensNumber, int yTokensNumber, double threshold) {
        return (int) Math.ceil((float) (threshold / 2 * (xTokensNumber + yTokensNumber)));
    }

    public boolean computableViaOverlap() {
        return true;
    }

    public double getRuntimeApproximation(double mappingSize) {
        return mappingSize / 1000d;
    }
}
