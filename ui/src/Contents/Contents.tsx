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
import React, { useEffect, useState } from "react";
import { Card } from "react-bootstrap";
import {
  api,
  Content,
  ContentKey,
  GetMultipleContentsRequest,
  GetMultipleContentsResponse,
} from "../utils";
import { factory } from "../ConfigLog4j";
import { ContentView } from "../ContentView";
import { useLocation, useParams } from "react-router-dom";

const log = factory.getLogger("api.TableListing");

const fetchContent = (
  ref: string,
  getMultipleContentsRequest: GetMultipleContentsRequest
): Promise<void | GetMultipleContentsResponse | undefined> => {
  return api()
    .getMultipleContents({
      ref,
      getMultipleContentsRequest,
    })
    .then((data) => {
      return data;
    })
    .catch((e) => log.error(`getMultipleContents ${JSON.stringify(e)}`));
};

const Contents = (): React.ReactElement => {
  const location = useLocation();
  const { branch, "*": path } = useParams<{ branch: string; "*": string }>();
  const [content, setContent] = useState<Content>();

  useEffect(() => {
    const getContent = async () => {
      const keyObj = { elements: path?.split("/") } as ContentKey;

      const contentData = await fetchContent(branch as string, {
        requestedKeys: [keyObj],
      });
      const contentList = contentData?.contents || [];
      const contentDetails =
        contentList.length > 0 ? contentList[0].content : undefined;
      setContent(contentDetails);
    };
    void getContent();
  }, [location]);

  return (
    <Card>
      <ContentView tableContent={content} />
    </Card>
  );
};

export default Contents;
