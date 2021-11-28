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

import io.cdap.cdap.api.service.AbstractService;

@SuppressWarnings("rawtypes")
public class AnonService extends AbstractService {
    /*
     * [AnonService] is a wrapper for the Anon handler
     * that exposes the public REST API of the Anon App.
     */
    public final String SERVICE_NAME = "AnonService";
    public final String SERVICE_DESC = "A common Cy-IoT service that provides the public REST API" +
        " for the Works. anomaly detection app.";

    private final AnonApp.AnonConfig config;

    public AnonService(AnonApp.AnonConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        this.setName(SERVICE_NAME);
        this.setDescription(SERVICE_DESC);

        this.addHandler(new AnonHandler(config));

    }
}
