package org.aksw.limes.core.measures.measure.string;

import static org.apache.commons.lang3.math.NumberUtils.min;

import org.aksw.limes.core.io.cache.Instance;
import org.apache.commons.lang3.math.NumberUtils;

public class SimpleEditDistanceMeasure extends StringMeasure {

  private final int matchingCost;
  private final int insertionCost;
  private final int deletionCost;

  public SimpleEditDistanceMeasure(int matchingCost, int insertionCost, int deletionCost) {
    this.matchingCost = matchingCost;
    this.insertionCost = insertionCost;
    this.deletionCost = deletionCost;
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

  public int getWorstCaseCost(int length1, int length2) {
    int min = Math.min(length1, length2);
    int result = min * Math.min(insertionCost+deletionCost, matchingCost);
    int lengthDifference = length1 - length2;
    if (lengthDifference > 0) {
      result += lengthDifference * deletionCost;
    } else {
      result += -lengthDifference * insertionCost;
    }
    return result;
  }

  @Override
  public double getSimilarity(Object object1, Object object2) {
    String s1 = object1 + "";
    String s2 = object2 + "";
    if (s1.isEmpty() && s2.isEmpty()) {
      return 1.0;
    }
    int length1 = s1.length(), length2 = s2.length();
    int[] previousRow = new int[length1 + 1];
    for (int i = 0; i <= length1; i++) {
      previousRow[i] = i * deletionCost;
    }
    for (int y = 0; y < length2; y++) {
      int[] currentRow = new int[length1+1];
      currentRow[0] = (y + 1) * insertionCost;
      char c2 = s2.charAt(y);
      for (int x = 0; x < length1; x++) {
        char c1 = s1.charAt(x);
        if (c1 == c2) {
          currentRow[x+1] = previousRow[x];
        } else {
          currentRow[x+1] = NumberUtils.min(previousRow[x] + matchingCost,
              previousRow[x+1] + insertionCost,
              currentRow[x] + deletionCost);
        }
      }
      previousRow = currentRow;
    }
    return 1.0 - previousRow[length1] / (double) getWorstCaseCost(length1, length2);
  }

  @Override
  public double getSimilarity(Instance instance1, Instance instance2, String property1,
      String property2) {
    double sim = 0;
    double max = 0;
    for (String p1 : instance1.getProperty(property1)) {
      for (String p2 : instance2.getProperty(property2)) {
        sim = getSimilarity(p1, p2);
        if (max < sim) {
          max = sim;
        }
      }
    }
    return max;
  }

  @Override
  public String getType() {
    return "string";
  }

  @Override
  public String getName() {
    return "simple-edit-distance";
  }

  @Override
  public boolean computableViaOverlap() {
    return false;
  }

  @Override
  public double getRuntimeApproximation(double mappingSize) {
    return mappingSize / 1000d;
  }

}
