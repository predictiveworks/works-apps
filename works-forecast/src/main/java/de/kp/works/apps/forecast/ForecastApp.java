package de.kp.works.apps.forecast;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;

import javax.annotation.Nullable;

public class ForecastApp extends AbstractApplication<ForecastApp.ForecastConfig> {

    public final String APP_NAME = "ForecastApp";
    public final String APP_DESC = "A Cy-IoT common application for multi-variant timeseries forecasting.";

    public static class ForecastConfig extends Config {
        /*
         * The name of the datasource that is used to
         * extract data from. The current implementation
         * supports ['ignite', postgres'].
         *
         * Default value: 'postgres'
         */
        @Nullable
        public final String dsName;
        /*
         * The (JDBC) URL to access the specified datasource
         */
        public final String dsUrl;
        /*
         * The optional registered user name of the datasource
         */
        @Nullable
        public final String dsUser;
        /*
         * The optional password of the registered user name
         * of the datasource
         */
        @Nullable
        public final String dsPassword;
        /*
         * The optional algorithm name of the forecast model.
         *
         * Default is 'LSTM'.
         */
        @Nullable final String modelAlgo;
        /*
         * The optional model type of the forecast model.
         * Supported values are 'DL', 'ML', 'TS'.
         *
         * Default is 'DL'.
         */
        @Nullable final String modelType;

        @SuppressWarnings("unused")
        public ForecastConfig(String dsUrl) {
            this(null, dsUrl, null, null, null, null);
        }

        @SuppressWarnings("unused")
        public ForecastConfig(
                String dsUrl,
                @Nullable String dsUser,
                @Nullable String dsPassword,
                @Nullable String modelAlgo,
                @Nullable String modelType) {

            this(null, dsUrl, dsUser, dsPassword, modelAlgo, modelType);

        }

        public ForecastConfig(
                @Nullable String dsName,
                String dsUrl,
                @Nullable String dsUser,
                @Nullable String dsPassword,
                @Nullable String modelAlgo,
                @Nullable String modelType) {

            this.dsName = dsName == null ? "postgres" : dsName;
            this.dsUrl = dsUrl;

            this.dsUser = dsUser;
            this.dsPassword = dsPassword;

            this.modelAlgo = modelAlgo == null ? "LSTM" : modelAlgo;
            this.modelType = modelType == null ? "DL" : modelType;
        }

    }

    @Override
    public void configure() {

        this.setName(APP_NAME);
        this.setDescription(APP_DESC);

        /* Retrieve the forecast configuration */
        ForecastConfig config = getConfig();

        /* Append the REST API to the forecast app */
        addService(new ForecastService(config));

        /* Append the Spark program that executes
         * timeseries forecasting
         */
        addSpark(new ForecastSpark(config));

    }
}
