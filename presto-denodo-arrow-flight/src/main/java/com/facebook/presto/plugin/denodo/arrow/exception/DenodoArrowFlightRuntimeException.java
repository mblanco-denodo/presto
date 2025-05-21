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
package com.facebook.presto.plugin.denodo.arrow.exception;

public class DenodoArrowFlightRuntimeException
        extends RuntimeException
{
    private static final long serialVersionUID = -6448705430789859448L;
    private final String message;
    private Exception cause;

    public DenodoArrowFlightRuntimeException(String message)
    {
        super(message);
        this.message = message;
    }

    public DenodoArrowFlightRuntimeException(String message, Exception cause)
    {
        super(message);
        this.message = message;
        this.cause = cause;
    }

    @Override
    public String getMessage()
    {
        return this.message;
    }

    @Override
    public Exception getCause()
    {
        return this.cause;
    }
}
