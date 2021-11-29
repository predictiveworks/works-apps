package de.kp.works.apps.imagery;
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

public class ImageryApp extends AbstractApplication<ImageryApp.ImageryConfig> {

    public final String APP_NAME = "ImageryApp";
    public final String APP_DESC = "A deep learning based application for imagery analytics.";

    public static class ImageryConfig extends Config {
    }

    @Override
    public void configure() {

        this.setName(APP_NAME);
        this.setDescription(APP_DESC);

        /* Retrieve the imagery configuration */
        ImageryConfig config = getConfig();

        /* Append the REST API to the imagery app */
        addService(new ImageryService(config));

    }

}
