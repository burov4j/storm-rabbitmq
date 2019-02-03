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
package ru.burov4j.storm.rabbitmq;

import java.util.Collections;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Andrey Burov
 */
public class ConfigFetcherTest {

    @Test
    public void fetchStringPropertyExists() {
        String propertyKey = "propertyKey",
                propertyValue = "propertyValue";
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchStringProperty(conf, propertyKey));
        assertEquals(propertyValue, ConfigFetcher.fetchStringProperty(conf, propertyKey, "defaultValue"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fetchStringPropertyNotExists() {
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey",
                propertyValue = "propertyValue",
                anotherValue = "anotherValue";
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchStringProperty(conf, anotherKey, anotherValue));
        ConfigFetcher.fetchStringProperty(conf, anotherKey);
    }

    @Test
    public void fetchIntegerPropertyExists() {
        String propertyKey = "propertyKey";
        int propertyValue = 445342;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchIntegerProperty(conf, propertyKey, 556634));
    }

    @Test
    public void fetchIntegerPropertyNotExists() {
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        int propertyValue = 445342,
                anotherValue = 556634;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchIntegerProperty(conf, anotherKey, anotherValue));
    }
    
    @Test
    public void fetchLongPropertyExists() {
        String propertyKey = "propertyKey";
        long propertyValue = 445342L;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchLongProperty(conf, propertyKey, 556634L));
    }

    @Test
    public void fetchLongPropertyNotExists() {
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        long propertyValue = 445342L,
                anotherValue = 556634L;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchLongProperty(conf, anotherKey, anotherValue));
    }
    
    @Test
    public void fetchIntegerPropertyButLong() {
        String propertyKey = "propertyKey";
        long propertyValue = 445342;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchIntegerProperty(conf, propertyKey, 10));
    }

    @Test
    public void fetchBooleanPropertyExists() {
        String propertyKey = "propertyKey";
        final boolean propertyValue = true;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(propertyValue, ConfigFetcher.fetchBooleanProperty(conf, propertyKey, false));
    }

    @Test
    public void fetchBooleanPropertyNotExists() {
        String propertyKey = "propertyKey",
                anotherKey = "anotherKey";
        final boolean propertyValue = true,
                anotherValue = false;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, propertyValue);
        assertEquals(anotherValue, ConfigFetcher.fetchBooleanProperty(conf, anotherKey, anotherValue));
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentException() {
        String propertyKey = "propertyKey";
        int illegalArgument = 0;
        Map<String, Object> conf = Collections.singletonMap(propertyKey, illegalArgument);
        ConfigFetcher.fetchStringProperty(conf, propertyKey);
    }
}
