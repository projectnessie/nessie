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
import CommitLog from "./CommitLog";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";

it("Commit log renders", async () => {
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
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("deadbeef")).toBeInTheDocument();
});
