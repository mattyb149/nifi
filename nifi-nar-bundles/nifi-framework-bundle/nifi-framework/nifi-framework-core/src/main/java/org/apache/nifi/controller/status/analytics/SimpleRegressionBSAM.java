/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.status.analytics;


import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;

public class SimpleRegressionBSAM extends BivariateStatusAnalyticsModel {

    private SimpleRegression regression;

    public SimpleRegressionBSAM() {
        this.regression = new SimpleRegression();
    }

    @Override
    public void learn(Stream<Double> features, Stream<Double> labels) {
        double[] labelArray = ArrayUtils.toPrimitive(labels.toArray(Double[]::new));
        double[][] featuresMatrix = features.map(feature -> new double[]{feature}).toArray(double[][]::new);
        regression.clear();
        regression.addObservations(featuresMatrix, labelArray);
    }

    @Override
    public Double predict(Double feature) {
        return predictY(feature);
    }

    @Override
    public Double predictX(Double y) {
        return (y - regression.getIntercept()) / regression.getSlope();
    }

    @Override
    public Double predictY(Double x) {
        return regression.getSlope() * x + regression.getIntercept();
    }
}
