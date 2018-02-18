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

import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author Andrey Burov
 */
public class ConfigFetcherTest {

    @Test
    public void fetchStringPropertyExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey",
                propertyValue = "propertyValue";
        conf.put(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchStringProperty(conf, propertyKey));
        assertEquals(propertyValue, ConfigFetcher.fetchStringProperty(conf, propertyKey, "defaultValue"));
    }

    @Test
    public void fetchStringPropertyNotExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey",
                propertyValue = "propertyValue",
                anotherValue = "anotherValue";
        conf.put(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchStringProperty(conf, anotherKey, anotherValue));
        try {
            ConfigFetcher.fetchStringProperty(conf, anotherKey);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException ex) {
            // the test passed
        }
    }

    @Test
    public void fetchIntegerPropertyExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey";
        int propertyValue = 445342;
        conf.put(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchIntegerProperty(conf, propertyKey, 556634));
    }

    @Test
    public void fetchIntegerPropertyNotExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        int propertyValue = 445342,
                anotherValue = 556634;
        conf.put(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchIntegerProperty(conf, anotherKey, anotherValue));
    }
    
    @Test
    public void fetchLongPropertyExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey";
        long propertyValue = 445342L;
        conf.put(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchLongProperty(conf, propertyKey, 556634L));
    }

    @Test
    public void fetchLongPropertyNotExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        long propertyValue = 445342L,
                anotherValue = 556634L;
        conf.put(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchLongProperty(conf, anotherKey, anotherValue));
    }
    
    @Test
    public void fetchIntegerPropertyButLong() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey";
        long propertyValue = 445342;
        conf.put(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchIntegerProperty(conf, propertyKey, 10));
    }

    @Test
    public void fetchBooleanPropertyExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey";
        final boolean propertyValue = true;
        conf.put(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchBooleanProperty(conf, propertyKey, false));
    }

    @Test
    public void fetchBooleanPropertyNotExists() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        final boolean propertyValue = true,
                anotherValue = false;
        conf.put(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchBooleanProperty(conf, anotherKey, anotherValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentException() {
        Map<String, Object> conf = new HashMap<>(1);
        String propertyKey = "propertyKey";
        int illegalArgument = 0;
        conf.put(propertyKey, illegalArgument);
        ConfigFetcher.fetchStringProperty(conf, propertyKey);
    }

}
