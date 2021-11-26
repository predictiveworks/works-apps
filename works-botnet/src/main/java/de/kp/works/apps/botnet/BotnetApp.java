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

import io.cdap.cdap.api.app.AbstractApplication;

@SuppressWarnings("rawtypes")
public class BotnetApp extends AbstractApplication {

    public final String APP_NAME = "BotnetApp";
    public final String APP_DESC = "A Cyber Defense specific application for detecting signals of botnet activities";

    @Override
    public void configure() {
        this.setName(APP_NAME);
        this.setDescription(APP_DESC);
        /*
         * Append the REST API to the Botnet Detection App
         */
        addService(new BotnetService());
    }
}
