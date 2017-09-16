/*
 * Copyright 2017 Andrey Burov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syberia.storm.rabbitmq;

import java.util.Map;

/**
 *
 * @author Andrey Burov
 */
class ConfigFetcher {

    private ConfigFetcher() {
        super();
    }

    public static String fetchStringProperty(Map<String, Object> config, String key) {
        return fetchStringProperty(config, key, null);
    }

    public static String fetchStringProperty(Map<String, Object> config, String key, String defaultValue) {
        return fetchProperty(config, key, defaultValue);
    }

    public static int fetchIntegerProperty(Map<String, Object> config, String key, int defaultValue) {
        return fetchProperty(config, key, defaultValue);
    }
    
    public static long fetchLongProperty(Map<String, Object> config, String key, long defaultValue) {
        return fetchProperty(config, key, defaultValue);
    }

    public static boolean fetchBooleanProperty(Map<String, Object> config, String key, boolean defaultValue) {
        return fetchProperty(config, key, defaultValue);
    }

    private static <T> T fetchProperty(Map<String, Object> config, String key, T defaultValue) {
        T value = (T) config.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new IllegalArgumentException("The property is not specified: " + key);
            } else {
                return defaultValue;
            }
        } else {
            return value;
        }
    }

}
