/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.denodo.arrow;

import com.facebook.plugin.arrow.ArrowConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

import static com.google.common.base.MoreObjects.firstNonNull;

public class DenodoArrowPlugin
        implements Plugin
{
    private static final String DENODO = "denodo-arrow";
    private final String name;
    private final Module module;
    private final ImmutableList<Module> extraModules;

    public DenodoArrowPlugin()
    {
        this.name = DENODO;
        this.module = new DenodoArrowModule(DENODO);
        this.extraModules = ImmutableList.of(module);
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), DenodoArrowPlugin.class.getClassLoader());
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ArrowConnectorFactory(name, module, extraModules, getClassLoader()));
    }
}
