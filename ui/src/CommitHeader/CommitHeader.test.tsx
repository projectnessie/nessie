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

import { render, screen, waitFor } from "@testing-library/react";
import CommitHeader from "./CommitHeader";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";

it("CommitHeader renders", async () => {
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
  const scope = nock("http://localhost/api/v1")
    .get("/trees/tree/main/log")
    .reply(200, { token: "foo", logEntries: [logEntry] });

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <CommitHeader currentRef={"main"} />
    </React.StrictMode>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("deadbeef"));

  scope.done();
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("deadbeef")).toBeInTheDocument();
});
