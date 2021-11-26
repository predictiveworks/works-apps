package de.kp.works.apps.botnet;
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
public class BotnetService extends AbstractService {
    /*
     * [BotnetService] is a wrapper for the Botnet handler
     * that exposes the public REST API of the Botnet App.
     */
    public final String SERVICE_NAME = "BotnetService";
    public final String SERVICE_DESC = "A Cyber Defense specific service that provides the public REST API" +
        " for the Works. botnet detection app.";

    @Override
    protected void configure() {

        this.setName(SERVICE_NAME);
        this.setDescription(SERVICE_DESC);

        this.addHandler(new BotnetHandler());

    }
}
