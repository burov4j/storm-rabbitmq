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

import java.util.Collections;
import java.util.List;

/**
 *
 * @author Andrey Burov
 */
@SuppressWarnings("WeakerAccess")
public final class StreamedTuple {
    
    private final String streamId;
    private final List<Object> tuple;

    public StreamedTuple(String streamId, List<Object> tuple) {
        this.streamId = streamId;
        this.tuple = Collections.unmodifiableList(tuple);
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }
}
