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
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";
import { App } from "./App";

it("ExploreTree renders", async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const entry = {
    name: { elements: ["a"] },
    type: "UNKNOWN",
  };

  const scope1 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/entries?namespaceDepth=1")
    .reply(200, { token: "foo", entries: [entry] });
  const scope2 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, {
      references: [
        tag("a"),
        tag("b"),
        branch("c"),
        branch("d"),
        branch("main"),
      ],
    });

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/tree/main"]}>
      <App />
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("Commit History"));

  scope1.done();
  scope2.done();
  scope3.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("a").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a"
  );
});

it("ExploreTree renders commit", async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);

  const logEntry = {
    commitMeta: {
      hash: "deadbeef",
      author: "bob",
      commitTime: now,
      committer: "sally",
      message: "commitMessage",
      properties: { a: "b", c: "d" },
    },
  };

  const scope2 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, {
      references: [
        tag("a"),
        tag("b"),
        branch("c"),
        branch("d"),
        branch("main"),
      ],
    });
  const scope4 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log?maxRecords=10")
    .reply(200, { token: "foo", logEntries: [logEntry] });

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/commits/main"]}>
      <App />
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("deadbeef"));

  scope2.done();
  scope3.done();
  scope4.done();
  // expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("commitMessage").closest("a")).toHaveAttribute(
    "href",
    "/commits/main/deadbeef"
  );
});

it("ExploreTree renders commit message", async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);

  const logEntry = {
    commitMeta: {
      hash: "deadbeef",
      author: "bob",
      commitTime: now,
      committer: "sally",
      message: "commitMessage",
      properties: { a: "b", c: "d" },
    },
  };

  const scope2 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, {
      references: [
        tag("a"),
        tag("b"),
        branch("c"),
        branch("d"),
        branch("main"),
      ],
    });
  const scope4 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log?maxRecords=10")
    .reply(200, { token: "foo", logEntries: [logEntry] });

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/commits/main/deadbeef"]}>
      <App />
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("sally"));

  scope2.done();
  scope3.done();
  scope4.done();
  expect(asFragment()).toMatchSnapshot();
});

it("ExploreTree renders with slug", async () => {
  const now = new Date();
  now.setDate(now.getDate() - 1);
  const entry = {
    name: { elements: ["b"] },
    type: "UNKNOWN",
  };

  const scope1 = nock("http://localhost/api/v1")
    .get(
      "/trees/tree/main/entries?filter=entry.namespace.matches(%27%5Ea(%5C%5C.%7C%24)%27)&namespaceDepth=2"
    )
    .reply(200, { token: "foo", entries: [entry] });
  const scope2 = nock("http://localhost/api/v1")
    .get("/trees/tree")
    .reply(200, { name: "main", hash: "deadbeef" });
  const scope3 = nock("http://localhost/api/v1")
    .get("/trees")
    .reply(200, {
      references: [
        tag("a"),
        tag("b"),
        branch("c"),
        branch("d"),
        branch("main"),
      ],
    });

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/tree/main/a"]}>
      <App />
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("a"));

  scope1.done();
  scope2.done();
  scope3.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("b").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a/b"
  );
});

const tag = (name: string, hash = "deadbeef") => {
  return { name, hash, type: "TAG" };
};

const branch = (name: string, hash = "deadbeef") => {
  return { name, hash, type: "BRANCH" };
};
