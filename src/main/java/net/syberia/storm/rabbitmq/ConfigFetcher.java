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
        // no operation
    }

    public static String fetchStringProperty(Map config, String key) {
        return fetchStringProperty(config, key, null);
    }

    public static String fetchStringProperty(Map config, String key, String defaultValue) {
        return fetchProperty(config, key, String.class, defaultValue);
    }

    public static int fetchIntegerProperty(Map config, String key, int defaultValue) {
        return fetchNumberProperty(config, key, defaultValue).intValue();
    }
    
    public static long fetchLongProperty(Map config, String key, long defaultValue) {
        return fetchNumberProperty(config, key, defaultValue).longValue();
    }
    
    private static Number fetchNumberProperty(Map config, String key, Number defaultValue) {
        return fetchProperty(config, key, Number.class, defaultValue);
    }

    public static boolean fetchBooleanProperty(Map config, String key, boolean defaultValue) {
        return fetchProperty(config, key, Boolean.class, defaultValue);
    }

    private static <T> T fetchProperty(Map config, String key, Class<T> valueClass, T defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new IllegalArgumentException("The property is not specified: " + key);
            } else {
                return defaultValue;
            }
        } else {
            try {
                return valueClass.cast(value);
            } catch (ClassCastException ex) {
                throw new IllegalArgumentException("Unable to fetch property: " + key, ex);
            }
        }
    }

}
