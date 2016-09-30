/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.aksw.limes.core.ml.algorithm.euclid;

import java.util.Comparator;

/**
 * @author Axel-C. Ngonga Ngomo (ngonga@informatik.uni-leipzig.de)
 */
public class SimpleClassifier implements Comparator<SimpleClassifier>{
    public String measure = null;
    public String sourceProperty = null;
    public String targetProperty = null;
    public double threshold = 1.0;
    public double weight = 1.0;
    protected double fMeasure = 0.0;

    public SimpleClassifier(String measure, double threshold) {
        this.measure = measure;
        this.threshold = threshold;
    }

    public SimpleClassifier(String measure, double threshold, String sourceProperty, String targetProperty) {
        this.measure = measure;
        this.threshold = threshold;
        this.sourceProperty = sourceProperty;
        this.targetProperty = targetProperty;
    }

    public SimpleClassifier clone() {
        SimpleClassifier copy = new SimpleClassifier(measure, threshold);
        copy.setfMeasure(fMeasure);
        copy.sourceProperty = sourceProperty;
        copy.targetProperty = targetProperty;
        copy.weight = weight;
        return copy;
    }

    public double getfMeasure() {
        return fMeasure;
    }

    public void setfMeasure(double fMeasure) {
        this.fMeasure = fMeasure;
    }

    public String getMeasure() {
        return measure;
    }

    public void setMeasure(String measure) {
        this.measure = measure;
    }

    /**
     * @return MetricExpression
     * @author sherif
     */
    public String getMetricExpression() {
        return measure + "(x." + sourceProperty + ",y." + targetProperty + ")|" + String.format("%.2f", threshold);
    }

    public String getSourceProperty() {
        return sourceProperty;
    }

    public void setSourceProperty(String sourceProperty) {
        this.sourceProperty = sourceProperty;
    }

    public String getTargetProperty() {
        return targetProperty;
    }

    public void setTargetProperty(String targetProperty) {
        this.targetProperty = targetProperty;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "(Source: " + sourceProperty + " Target: " + targetProperty + " Measure: " + measure + " Theta = " + threshold + " FMeasure = " + getfMeasure() + " Weight = " + weight + ")";
    }

    /**
     * Shorter toString().
     *
     * @return m(p1, p2) theta=t, weight=w.
     */
    public String toString2() {
        return "" + measure + "(" + sourceProperty + " , " + targetProperty + "):Theta = " + threshold + ", Weight = " + weight;
    }
    
    
    @Override
    public int compare(SimpleClassifier o1, SimpleClassifier o2) {
        if (o1.getfMeasure() > o2.getfMeasure()) {
            return 1;
        }
        if (o1.getfMeasure() < o2.getfMeasure()) {
            return -1;
        }
        return 0;
    }
}
