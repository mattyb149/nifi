package org.apache.nifi.controller.status.analytics;

import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecursiveLeastSquaresBSAM extends BivariateStatusAnalyticsModel{

    private static final Logger LOG = LoggerFactory.getLogger(RecursiveLeastSquaresBSAM.class);
    private smile.regression.RLS rlsModel;
    private final Boolean clearObservationsOnLearn;

    public RecursiveLeastSquaresBSAM(Boolean clearObservationsOnLearn) {
        this.clearObservationsOnLearn = clearObservationsOnLearn;
    }

    @Override
    public void learn(Stream<Double> features, Stream<Double> labels) {

        double[] labelArray = ArrayUtils.toPrimitive(labels.toArray(Double[]::new));
        double[][] featuresMatrix = features.map(feature -> new double[]{feature}).toArray(double[][]::new);
        if(rlsModel == null || this.clearObservationsOnLearn){
            this.rlsModel = new smile.regression.RLS(featuresMatrix,labelArray,.5);
        }else{
            this.rlsModel.learn(featuresMatrix,labelArray);
        }

        LOG.debug("Total number of observations: {}",featuresMatrix.length);
        LOG.debug("Coefficients: {}", rlsModel.coefficients());

    }

    @Override
    public Double predict(Double feature) {
        return predictY(feature);
    }

    @Override
    public Double predictX(Double y) {
        if(rlsModel == null){
            return Double.NaN;
        }

        final double[] coefficients = rlsModel.coefficients();
        double slope = coefficients[0];
        double intercept = coefficients[1];
        return (y - intercept) / slope;

    }

    @Override
    public Double predictY(Double x) {
        if(rlsModel == null) {
            return Double.NaN;
        }

        return rlsModel.predict(new double[]{x});
    }


}
