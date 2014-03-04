/*
 * Copyright 2014 Sean Kerr
 *
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

package org.simplebase.model;

import java.io.IOException;

/**
 * {@link ModelException} is thrown when a {@link Model} error occurs.
 *
 * @author Sean Kerr [sean@code-box.org]
 */
public class ModelException extends IOException {
    /**
     * Create a new ModelException instance.
     *
     * @param format The error format.
     * @param args   The format arguments.
     */
    public ModelException (String format, Object... args) {
        super(String.format(format, args));
    }
}