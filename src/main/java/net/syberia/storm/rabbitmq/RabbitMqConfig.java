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

import com.rabbitmq.client.ConnectionFactory;
import java.io.Serializable;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

/**
 *
 * @author Andrey Burov
 */
@EqualsAndHashCode
public class RabbitMqConfig implements Serializable {

    private static final long serialVersionUID = 5275552557426635758L;

    public static final String KEY_HOST = "rabbitmq.host";
    public static final String KEY_PORT = "rabbitmq.port";
    public static final String KEY_ADDRESSES = "rabbitmq.addresses";
    public static final String KEY_USERNAME = "rabbitmq.username";
    public static final String KEY_PASSWORD = "rabbitmq.password";
    public static final String KEY_VIRTUAL_HOST = "rabbitmq.virtual_host";
    public static final String KEY_REQUESTED_HEARTBEAT = "rabbitmq.requested_heartbeat";

    private String host = ConnectionFactory.DEFAULT_HOST;
    private int port = ConnectionFactory.DEFAULT_AMQP_PORT;
    private String addresses = StringUtils.EMPTY;
    private String username = ConnectionFactory.DEFAULT_USER;
    private String password = ConnectionFactory.DEFAULT_PASS;
    private String virtualHost = ConnectionFactory.DEFAULT_VHOST;
    private int requestedHeartbeat = ConnectionFactory.DEFAULT_HEARTBEAT;

    RabbitMqConfig() {
        super();
    }

    public RabbitMqConfig(Map<String, Object> rabbitMqConfig) {
        this.host = ConfigFetcher.fetchStringProperty(rabbitMqConfig, KEY_HOST, this.host);
        this.port = ConfigFetcher.fetchIntegerProperty(rabbitMqConfig, KEY_PORT, this.port);
        this.addresses = ConfigFetcher.fetchStringProperty(rabbitMqConfig, KEY_ADDRESSES, this.addresses);
        this.username = ConfigFetcher.fetchStringProperty(rabbitMqConfig, KEY_USERNAME, this.username);
        this.password = ConfigFetcher.fetchStringProperty(rabbitMqConfig, KEY_PASSWORD, this.password);
        this.virtualHost = ConfigFetcher.fetchStringProperty(rabbitMqConfig, KEY_VIRTUAL_HOST, this.virtualHost);
        this.requestedHeartbeat = ConfigFetcher.fetchIntegerProperty(rabbitMqConfig, KEY_REQUESTED_HEARTBEAT, this.requestedHeartbeat);
    }

    public String getHost() {
        return host;
    }

    void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    void setPort(int port) {
        this.port = port;
    }

    public boolean hasAddresses() {
        return !addresses.isEmpty();
    }

    public String getAddresses() {
        return addresses;
    }

    void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    public String getUsername() {
        return username;
    }

    void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public int getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    void setRequestedHeartbeat(int requestedHeartbeat) {
        this.requestedHeartbeat = requestedHeartbeat;
    }

}
