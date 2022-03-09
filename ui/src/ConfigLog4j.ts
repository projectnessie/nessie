/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { LogLevel } from "typescript-logging";
import { Log4TSProvider } from "typescript-logging-log4ts-style";

// Create provider and specify 2 log groups:
// * One for any logger with a name starting with model, to log on error
// * The second one for anything else to log on info
export const logProvider = Log4TSProvider.createProvider(
  "NessieLog4TSProvider",
  {
    level: LogLevel.Debug,
    groups: [
      {
        expression: new RegExp("api.+"),
        level: LogLevel.Error,
      },
      {
        expression: new RegExp(".+"),
        level: LogLevel.Info,
      },
    ],
  }
);
