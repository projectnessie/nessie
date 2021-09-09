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
import { BrowserRouter, Route, Router } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import Explore from "./Explore";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";
import { createMemoryHistory } from "history";

// noinspection TypeScriptValidateTypes
const renderWithRouter = (component: React.ReactElement) => {
  const history = createMemoryHistory({
    initialEntries: ["/tree/main/a"],
  });
  // eslint-disable-next-line react/prop-types
  const Wrapper = ({ children }) => (
    <Router history={history}>
      <Route path="/tree/:slug">{children}</Route>
    </Router>
  );
  return {
    ...render(component, { wrapper: Wrapper }),
    history,
  };
};

it("Explore renders", async () => {
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const commitMeta = {
    hash: "deadbeef",
    author: "bob",
    commitTime: now,
    committer: "sally",
    message: "commitMessage",
    properties: { a: "b", c: "d" },
  };
  const scope1 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log")
    .reply(200, { token: "foo", operations: [commitMeta] });
  const scope2 = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&query_expression=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope4 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, [tag("a"), tag("b"), branch("c"), branch("d")]);

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <BrowserRouter>
        <Explore />
      </BrowserRouter>
    </React.StrictMode>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("a"));

  scope1.done();
  scope2.done();
  scope3.done();
  scope4.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("a").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a"
  );
});

it("Explore renders with slug", async () => {
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const commitMeta = {
    hash: "deadbeef",
    author: "bob",
    commitTime: now,
    committer: "sally",
    message: "commitMessage",
    properties: { a: "b", c: "d" },
  };
  const scope1 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log")
    .reply(200, { token: "foo", operations: [commitMeta] });
  const scope2 = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?namespaceDepth=1&query_expression=entry.namespace.matches(%27(%5C%5C.%7C%24)%27)"
    )
    .reply(200, { token: "foo", entries: [entry] });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope4 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, [tag("a"), tag("b"), branch("c"), branch("d")]);
  // jest.spyOn(MemoryRouter, 'useParams').mockReturnValue({ slug: "/main/a" });

  const { getByText, asFragment } = renderWithRouter(<Explore />);
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("a"));

  scope1.done();
  scope2.done();
  scope3.done();
  scope4.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("a").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a"
  );
});

const tag = (name: string, hash = "deadbeef") => {
  return { name, hash, type: "TAG" };
};

const branch = (name: string, hash = "deadbeef") => {
  return { name, hash, type: "BRANCH" };
};
