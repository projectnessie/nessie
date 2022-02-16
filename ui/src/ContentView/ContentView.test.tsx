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
import ContentView from "./ContentView";
// tslint:disable-next-line:no-implicit-dependencies
import nock from "nock";
import { Content } from "../utils";

it("Render Iceberg Content", async () => {
  const scope = nock("http://localhost/api/v1")
    .get(
      "/contents?hashOnRef=55cc2e40cbe2eed190f3987c53ffc34643d5cfa5ef878392d88b2eb4137e92e4&ref=dev"
    )
    .reply(200, { contentList });
  const { getByAltText, asFragment } = render(
    <BrowserRouter>
      <ContentView tableContent={icebergTableContent} />
    </BrowserRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByAltText("IcebergTable"));
  setTimeout(() => {
    scope.done();
  }, 5000);
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByAltText("IcebergTable")).toHaveAttribute(
    "src",
    "/iceberg.png"
  );
});

it("Render DeltaLake Content", async () => {
  const scope = nock("http://localhost/api/v1")
    .get(
      "/contents?hashOnRef=55cc2e40cbe2eed190f3987c53ffc34643d5cfa5ef878392d88b2eb4137e92e4&ref=dev"
    )
    .reply(200, { contentList });
  const { getByAltText, asFragment } = render(
    <BrowserRouter>
      <ContentView tableContent={deltaLakeContent} />
    </BrowserRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByAltText("IcebergTable"));
  setTimeout(() => {
    scope.done();
  }, 5000);
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByAltText("IcebergTable")).toHaveAttribute(
    "src",
    "/delta.png"
  );
});

it("Render Iceberg View Content", async () => {
  const scope = nock("http://localhost/api/v1")
    .get(
      "/contents?hashOnRef=55cc2e40cbe2eed190f3987c53ffc34643d5cfa5ef878392d88b2eb4137e92e4&ref=dev"
    )
    .reply(200, { contentList });
  const { getByText, asFragment } = render(
    <BrowserRouter>
      <ContentView tableContent={icebergViewContent} />
    </BrowserRouter>
  );
  expect(asFragment()).toMatchSnapshot();
  await waitFor(() => getByText("ICEBERG_VIEW"));
  setTimeout(() => {
    scope.done();
  }, 5000);
  expect(asFragment()).toMatchSnapshot();
  expect(screen.getByText("ICEBERG_VIEW")).toBeInTheDocument();
});

const icebergTableContent: Content = {
  type: "ICEBERG_TABLE",
  id: "5d8bdfec-96d7-44f6-8770-1ffcf09e044d",
  metadataLocation: "/path/to/metadata/",
  snapshotId: 32,
  schemaId: 123,
  specId: 231,
  sortOrderId: 1,
};

const icebergViewContent: Content = {
  type: "ICEBERG_VIEW",
  id: "ae63654e-e692-49b2-8cd6-21e0d980893c",
  metadataLocation: "/path/to/view_metadata/",
  versionId: 1,
  schemaId: 123,
  sqlText: "CREATE VIEW test.v AS SELECT * FROM t",
  dialect: "Hive",
};

const deltaLakeContent: Content = {
  type: "DELTA_LAKE_TABLE",
  id: "da62c210-ffa1-4fbe-b63e-fa8978ecef1c",
  metadataLocationHistory: [
    "/path/to/delta/_delta_log/00000000000000000020.log",
    "/path/to/delta/_delta_log/00000000000000000010.log",
  ],
  checkpointLocationHistory: [
    "/path/to/delta/_delta_log/00000000000000000010.checkpoint.parquet",
    "/path/to/delta/_delta_log/00000000000000000020.checkpoint.parquet",
  ],
  lastCheckpoint: "/path/to/delta/_delta_log/_last_checkpoint",
};

const contentList = {
  contents: [
    {
      key: {
        elements: ["table2", "key"],
      },
      content: icebergTableContent,
    },
    {
      key: {
        elements: ["delatalake", "key"],
      },
      content: deltaLakeContent,
    },
    {
      key: {
        elements: ["example", "key"],
      },
      content: icebergViewContent,
    },
  ],
};
