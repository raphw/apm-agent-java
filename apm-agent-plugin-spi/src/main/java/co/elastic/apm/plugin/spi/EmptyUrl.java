/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package co.elastic.apm.plugin.spi;

import javax.annotation.Nullable;
import java.net.URI;

public class EmptyUrl implements Url {

    public static final Url INSTANCE = new EmptyUrl();

    private EmptyUrl() {
    }

    @Override
    public void fillFrom(URI uri) {
    }

    @Override
    public void fillFrom(@Nullable String scheme, @Nullable String serverName, int serverPort, @Nullable String requestURI, @Nullable String queryString) {
    }

    @Override
    public Url withProtocol(@Nullable String protocol) {
        return this;
    }

    @Override
    public Url withHostname(@Nullable String hostname) {
        return this;
    }

    @Override
    public Url withPort(int port) {
        return this;
    }

    @Override
    public Url withPathname(@Nullable String pathname) {
        return this;
    }

    @Override
    public Url withSearch(@Nullable String search) {
        return this;
    }
}
