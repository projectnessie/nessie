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
import { render, screen } from "@testing-library/react";
import ExploreLink from "./ExploreLink";

it("ExploreLink renders with container", () => {
  const { asFragment } = render(
    <BrowserRouter>
      <ExploreLink path={["a", "b", "c"]} toRef={"main"}>
        xxx
      </ExploreLink>
    </BrowserRouter>
  );

  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("xxx").closest("a")).toHaveAttribute(
    "href",
    "/tree/main/a/b/c"
  );
});

it("ExploreLink renders with object", () => {
  const { asFragment } = render(
    <React.StrictMode>
      <BrowserRouter>
        <ExploreLink path={undefined} toRef={"main"} type={"OBJECT"}>
          xxx
        </ExploreLink>
      </BrowserRouter>
    </React.StrictMode>
  );

  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("xxx").closest("a")).toHaveAttribute(
    "href",
    "/contents/main/"
  );
});
