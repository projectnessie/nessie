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
import {
  BrowserRouter,
  MemoryRouter,
  Routes,
  Route,
  useLocation,
} from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import CommitLog from "./CommitLog";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";

const ShowPath = () => {
  const { pathname, search, hash } = useLocation();
  return <pre>{JSON.stringify({ pathname, search, hash })}</pre>;
};

it("Commit log renders", async () => {
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
  const scope1 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log?maxRecords=10")
    .reply(200, { token: "foo", logEntries: [logEntry] });

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <BrowserRouter>
        <CommitLog currentRef={"main"} path={["main"]} />
      </BrowserRouter>
    </React.StrictMode>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("deadbeef"));

  scope1.done();
  expect(screen.getByText("deadbeef")).toBeInTheDocument();
});

it("Commit redirects on an invalid ref", async () => {
  const scope1 = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log?maxRecords=10")
    .reply(404);

  const { getByText, asFragment } = render(
    <MemoryRouter initialEntries={["/tree/main"]}>
      <Routes>
        <Route
          path="/tree/*"
          element={<CommitLog currentRef={"main"} path={["main"]} />}
        />
        <Route path={"/notfound"} element={<ShowPath />} />
      </Routes>
    </MemoryRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() =>
    getByText((content, element) => {
      return element?.tagName.toLowerCase() === "div";
    })
  );

  scope1.done();
  expect(asFragment()).toMatchInlineSnapshot(`
    <DocumentFragment>
      <pre>
        {"pathname":"/notfound","search":"","hash":""}
      </pre>
    </DocumentFragment>
  `);
});
