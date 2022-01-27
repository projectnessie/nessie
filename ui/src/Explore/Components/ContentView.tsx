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

import React, { Fragment } from "react";
import "./ContentView.css";
import {
  Content,
  DeltaLakeTable,
  IcebergTable,
  IcebergView,
} from "../../utils";
import { EmptyMessageView } from ".";

const ContentView = (props: { tableContent: Content }): React.ReactElement => {
  const { tableContent } = props;

  const imageView = (imgPath: string) => {
    return (
      <div className={"ContentView__imagetWrapper"}>
        <img alt="IcebergTable" src={`${imgPath}`} width="180" height="180" />
      </div>
    );
  };

  const renderDeltaView = (deltaContent: DeltaLakeTable) => {
    const {
      metadataLocationHistory,
      checkpointLocationHistory,
      lastCheckpoint,
    } = deltaContent;
    return (
      <div className={"ContentView"}>
        <div className={"ContentView__contentWrapper"}>
          <div>
            <span>Type:</span>
            <span className={"ml-2"}> {tableContent.type}</span>
          </div>
          {metadataLocationHistory && (
            <div>
              <span>Metadata Location History:</span>
              <ul>
                {metadataLocationHistory.map((metaLoc, index) => {
                  return (
                    <li className={"ml-4"} key={`${index}_metaHistory`}>
                      {metaLoc}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
          {checkpointLocationHistory && (
            <div>
              <span>Checkpoint History:</span>
              <ul>
                {checkpointLocationHistory.map((checkPointHist, index) => {
                  return (
                    <li className={"ml-4"} key={`${index}_checkPointHistory`}>
                      {checkPointHist}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
          {lastCheckpoint && (
            <div>
              <span>Last Checkpoint:</span>
              <span className={"ml-2"}>{lastCheckpoint}</span>
            </div>
          )}
        </div>
        {imageView("/delta.png")}
      </div>
    );
  };

  const renderIcebergTableView = (icebergContent: IcebergTable) => {
    const { metadataLocation } = icebergContent;
    return (
      <div className={"ContentView"}>
        <div className={"ContentView__contentWrapper"}>
          <div>
            <span>Type:</span>
            <span className={"ml-2"}> {tableContent.type}</span>
          </div>
          {metadataLocation && (
            <div>
              <span>Metadata Location:</span>
              <span className={"ml-2"}>{metadataLocation}</span>
            </div>
          )}
        </div>
        {imageView("/iceberg.png")}
      </div>
    );
  };

  const renderIcebergView = (icebergViewContent: IcebergView) => {
    const { metadataLocation, versionId, schemaId, sqlText, dialect } =
      icebergViewContent;
    return (
      <div className={"ContentView"}>
        <div className={"ContentView__contentWrapper"}>
          <div>
            <span>Type:</span>
            <span className={"ml-2"}> {tableContent.type}</span>
          </div>
          {metadataLocation && (
            <div>
              <span>Metadata Location:</span>
              <span className={"ml-2"}>{metadataLocation}</span>
            </div>
          )}
          {versionId && (
            <div>
              <span>Version ID:</span>
              <span className={"ml-2"}>{versionId}</span>
            </div>
          )}
          {schemaId && (
            <div>
              <span>Schema ID:</span>
              <span className={"ml-2"}>{schemaId}</span>
            </div>
          )}
          {sqlText && (
            <div>
              <span>Sql Text:</span>
              <span className={"ml-2"}>{sqlText}</span>
            </div>
          )}
          {dialect && (
            <div>
              <span>Dialect:</span>
              <span className={"ml-2"}>{dialect}</span>
            </div>
          )}
        </div>
        {imageView("/iceberg.png")}
      </div>
    );
  };

  const renderInvalidView = () => {
    return <EmptyMessageView message="Invalid type" />;
  };

  const renderContent = () => {
    switch (tableContent.type) {
      case "DELTA_LAKE_TABLE": {
        return renderDeltaView(tableContent);
      }
      case "ICEBERG_TABLE": {
        return renderIcebergTableView(tableContent);
      }
      case "ICEBERG_VIEW": {
        return renderIcebergView(tableContent);
      }
      default: {
        return renderInvalidView();
      }
    }
  };

  return <Fragment>{renderContent()}</Fragment>;
};

export default ContentView;
