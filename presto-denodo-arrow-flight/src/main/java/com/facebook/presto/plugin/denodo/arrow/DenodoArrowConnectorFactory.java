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
import com.facebook.presto.plugin.denodo.arrow.handler.DenodoArrowHandleResolver;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.google.inject.Module;

import java.util.List;

public class DenodoArrowConnectorFactory
        extends ArrowConnectorFactory
{
    public DenodoArrowConnectorFactory(String name, Module module,
            List<Module> extraModules, ClassLoader classLoader)
    {
        super(name, module, extraModules, classLoader);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new DenodoArrowHandleResolver();
    }
}
