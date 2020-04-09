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
const { createProxyMiddleware } = require('http-proxy-middleware');

let auth = "";

function relayRequestHeaders(proxyReq, req) {
  // proxyReq.setHeader('authorization', auth);
}

function relayResponseHeaders(proxyRes, req, res) {
  // auth = proxyRes.headers['authorization']
  // res.setHeader('authorization', proxyRes.headers['authorization']);
  // res.setHeader('x-test-auth', proxyRes.headers['authorization']);
}

module.exports = function(app) {
  app.use(
    '/api/v1/',
    createProxyMiddleware({
      target: 'http://localhost:19120',
      changeOrigin: true,
      onProxyReq: relayRequestHeaders,
      onProxyRes: relayResponseHeaders
    })
  );
};

