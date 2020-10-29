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
import PropTypes from "prop-types";
import {Link} from "react-router-dom";

function ExploreLink(props) {
  const path = props.path || [];
  const currentRef = props.toRef;
  const prefix = props.type === "CONTAINER" ? "/tree/" : "/contents/"
  return (
    <Link to={ `${prefix}${currentRef}/${path.join("/")}` } className={props.className} children={props.children}>
      {props.children}
    </Link>
  );
}

ExploreLink.propTypes = {
  type: PropTypes.oneOf(["CONTAINER", "OBJECT"]),
  toRef: PropTypes.string.isRequired,
  path: PropTypes.arrayOf(PropTypes.string).isRequired
}

ExploreLink.defaultProps = {
  type: "CONTAINER",
  path: []
}

export default ExploreLink;
