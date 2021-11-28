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

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.spark.AbstractSpark;

import java.util.Map;

/**
 * [AnonSpark] is a wrapper class for the [AnonReactor]
 */
public class AnonSpark extends AbstractSpark {

    public static final String NAME = "AnonReactor";
    public static final String DESC = "This Spark program detects anomalies in various data source" +
            " by supporting multiple anomaly models";

    AnonApp.AnonConfig config;

    public AnonSpark(AnonApp.AnonConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        this.setName(NAME);
        this.setDescription(DESC);
        /*
         * Extract client, driver and executor resources
         * from the provided application configuration
         * and, if exists, assign to the Spark program
         */
        Resources clientResources = getClientResources();
        if (clientResources != null) {
            this.setClientResources(clientResources);
        }

        Resources driverResources = getDriverResources();
        if (driverResources != null) {
            this.setDriverResources(driverResources);
        }

        Resources executorResources = getExecutorResources();
        if (executorResources != null) {
            this.setExecutorResources(executorResources);
        }
        /*
         * Transform the application configuration
         * into a property Map and make this Map
         * available for the [AnonReactor]
         */
        this.setProperties(getProperties());
        /*
         * Connect the Spark program with the [AnonReactor]
         */
        this.setMainClass(AnonReactor.class);
        this.setMainClassName(AnonReactor.class.getName());
    }
    /**
     * Helper method to retrieve client resources
     * from the provided application configuration
     */
    private Resources getClientResources() {
        return null;
    }
    /**
     * Helper method to retrieve driver resources
     * from the provided application configuration
     */
    private Resources getDriverResources() {
        return null;
    }
    /**
     * Helper method to retrieve executor resources
     * from the provided application configuration
     */
    private Resources getExecutorResources() {
        return null;
    }
    /**
     * This method transforms the application configuration
     * into a property map; this map is available in the
     * AnonReactor via the [SparkSpecification]
     */
    private Map<String,String> getProperties() {
        return null;
    }
}
