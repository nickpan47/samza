/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.samza.system;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.util.Util;


public class Systems {

  public static Collection<String> getSystemNames(Config config) {
    return new JavaSystemConfig(config).getSystemNames();
  }

  public static Map<String, SystemFactory> getSystemFactories(Config config) {
    Map<String, SystemFactory> systemFactories =
        getSystemNames(config).stream().collect(Collectors.toMap(systemName -> systemName, systemName -> {

            String systemFactoryClassName = new JavaSystemConfig(config).getSystemFactory(systemName);
            if (systemFactoryClassName == null) {
              throw new SamzaException(
                  String.format("A stream uses system %s, which is missing from the configuration.", systemName));
            }
            return Util.getObj(systemFactoryClassName);
          }));

    return systemFactories;
  }

  public static Map<String, SystemAdmin> getSystemAdmins(Config config) {
    return getSystemFactories(config).entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getAdmin(entry.getKey(), config)));
  }
}
