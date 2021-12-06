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

import React from "react";
import { BrowserRouter, Router } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import TableListing from "./TableListing";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";
import { createMemoryHistory } from "history";

it("TableListing renders", async () => {
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };
  const scope = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&filter=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <BrowserRouter>
      <TableListing currentRef={"main"} branches={tree} />
    </BrowserRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("a"));

  scope.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("a").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a"
  );
});

it("TableListing renders object", async () => {
  const entry = {
    name: { elements: ["b"] },
    type: "TABLE",
  };
  const scope = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&filter=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <BrowserRouter>
        <TableListing currentRef={"main"} branches={tree} />
      </BrowserRouter>
    </React.StrictMode>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("b"));

  scope.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("b").closest("a")).toHaveAttribute(
    "href",
    "/content/main/b"
  );
});

it("TableListing redirects on an invalid ref", async () => {
  const history = createMemoryHistory();
  const scope = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&filter=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(404);

  const { getByText, asFragment } = render(
    <Router history={history}>
      <TableListing currentRef={"main"} />
    </Router>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() =>
    getByText((content, element) => {
      return element?.tagName.toLowerCase() === "div";
    })
  );

  scope.done();
  expect(history.location.pathname).toEqual("/notfound");
});
const tree = [
  {
    type: "BRANCH",
    name: "main",
    hash: "6090dfaebc75b1ef94ea691dee3d43e08f6b1d60eb9b6286a1df49c8a8fe8f36",
  },
  {
    type: "BRANCH",
    name: "dev",
    hash: "b53925421f56fb8d1522e12f69d1fb939eaba71d110bf9b8b91516d06e6ff8d0",
  },
];
