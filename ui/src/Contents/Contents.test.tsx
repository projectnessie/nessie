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
import nock from "nock";
import { render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import TableListing from "../TableListing/TableListing";

it("TableListing renders object", async () => {
  const entry = {
    name: { elements: ["b"] },
    type: "TABLE",
  };
  const scope = nock("http://localhost/api/v1")
    .get("/trees/tree/main/entries?namespaceDepth=1")
    .reply(200, { token: "foo", entries: [entry] });

  const { getByText, asFragment } = render(
    <React.StrictMode>
      <MemoryRouter initialEntries={["/tree/main"]}>
        <Routes>
          <Route path={"/tree/:branch"} element={<TableListing />} />
        </Routes>
      </MemoryRouter>
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
