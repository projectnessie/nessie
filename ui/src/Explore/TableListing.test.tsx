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
import { BrowserRouter } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import TableListing from "./TableListing";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";

it("TableListing renders", async () => {
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };
  const scope = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&query_expression=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <BrowserRouter>
      <TableListing currentRef={"main"} />
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
      "/trees/tree/main/entries?namespaceDepth=1&query_expression=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <BrowserRouter>
        <TableListing currentRef={"main"} />
      </BrowserRouter>
    </React.StrictMode>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("b"));

  scope.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("b").closest("a")).toHaveAttribute(
    "href",
    "/contents/main/b"
  );
});
