package org.aksw.limes.core.gui.model.ml;

import javafx.concurrent.Task;

import org.aksw.limes.core.evaluation.qualititativeMeasures.PseudoFMeasure;
import org.aksw.limes.core.gui.model.Config;
import org.aksw.limes.core.io.cache.Cache;
import org.aksw.limes.core.ml.algorithm.MLResults;

public class UnsupervisedLearningModel extends MachineLearningModel {

    public UnsupervisedLearningModel(Config config, Cache sourceCache,
                                     Cache targetCache) {
        super(config, sourceCache, targetCache);
    }

    @Override
    public Task<Void> createLearningTask() {

        return new Task<Void>() {
            @Override
            protected Void call() {
        	MLResults model = null;
                try {
                    mlalgorithm.init(learningParameters, sourceCache, targetCache);
                    model = mlalgorithm.asUnsupervised().learn(new PseudoFMeasure());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                setLearnedMapping(mlalgorithm.predict(sourceCache, targetCache, model));
                return null;
            }
        };
    }

}
