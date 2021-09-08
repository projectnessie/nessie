/* eslint-disable @typescript-eslint/no-unsafe-call,@typescript-eslint/no-unsafe-assignment,@typescript-eslint/no-unsafe-member-access */
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
import { createProxyMiddleware } from "http-proxy-middleware";

// eslint-disable-next-line @typescript-eslint/no-var-requires,import/no-extraneous-dependencies
const express = require("express");
const app = express();

app.use(
  "/api/v1/",
  createProxyMiddleware({
    target: "http://localhost:19120",
    changeOrigin: true,
  })
);
app.listen(3000);
