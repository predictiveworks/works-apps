package de.kp.works.apps.anon;
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

public class AnonApp extends AbstractApplication<AnonApp.AnonConfig> {

    public final String APP_NAME = "AnonApp";
    public final String APP_DESC = "A Cy-IoT common application for detecting anomalous data signals.";

    public static class AnonConfig extends Config {
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

        public final String anonModel;

        @SuppressWarnings("unused")
        public AnonConfig(String dsUrl, String anonModel) {
            this(null, dsUrl, null, null, anonModel);
        }

        @SuppressWarnings("unused")
        public AnonConfig(String dsUrl, String dsUser, String dsPassword, String anonModel) {
            this(null, dsUrl, dsUser, dsPassword, anonModel);
        }

        public AnonConfig(
                @Nullable String dsName,
                String dsUrl,
                @Nullable String dsUser,
                @Nullable String dsPassword,
                String anonModel) {

            this.dsName = dsName == null ? "postgres" : dsName;
            this.dsUrl = dsUrl;

            this.dsUser = dsUser;
            this.dsPassword = dsPassword;

            this.anonModel = anonModel;

        }

    }

    @Override
    public void configure() {

        this.setName(APP_NAME);
        this.setDescription(APP_DESC);

        /* Retrieve the anomaly configuration */
        AnonConfig config = getConfig();

        /* Append the REST API to the anomaly app */
        addService(new AnonService(config));

        /* Append the Spark program that executes
         * anomaly detection
         */
        addSpark(new AnonSpark(config));

    }
}
