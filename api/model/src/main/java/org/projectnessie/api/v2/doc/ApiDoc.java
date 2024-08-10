/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v2.doc;

import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;

/** A collections of constants for defining OpenAPI annotations. */
public interface ApiDoc {

  String PAGING_INFO =
      "To implement paging, check 'hasMore' in the response and, if 'true', pass the value "
          + "returned as 'token' in the next invocation as the 'pageToken' parameter.\n"
          + "\n"
          + "The content and meaning of the returned 'token' is \"private\" to the implementation,"
          + "treat is as an opaque value.\n"
          + "\n"
          + "It is wrong to assume that invoking this method with a very high 'maxRecords' value "
          + "will return all available data in one page.\n"
          + "\n"
          + "Different pages may have different numbers of log records in them even if they come from another "
          + "call to this method with the same parameters. Also, pages are not guaranteed to be filled to "
          + "contain exactly 'maxRecords' even if the total amount of available data allows that. Pages may "
          + "contain more of less entries at server's discretion.\n";

  String FULL_REF_INFO =
      "The 'name@hash' form always refers to the exact commit on a specific named reference. This is the most complete "
          + "form of a reference. Other forms omit some of the details and require those gaps to be filled by the "
          + "server at runtime. Although these forms may be convenient to a human-being, they may resolve differently "
          + "at different times depending on the state of the system. Using the full 'name@hash' form is recommended "
          + "to avoid ambiguity.\n";

  String COMMIT_BRANCH_DESCRIPTION =
      "A reference to a particular version of the contents tree (a point in history) on a branch.\n"
          + "This reference is specified in this form:\n"
          + "- name@hash - Identifies the 'hash' commit on the named branch.\n"
          + "\n"
          + "The 'hash' commit must be reachable from the current HEAD of the branch.\n"
          + "In this case 'hash' indicates the state of contents that should be used for validating incoming changes.\n";

  String MERGE_TRANSPLANT_BRANCH_DESCRIPTION =
      "A reference to a specific version of the contents tree (a point in history) on a branch.\n"
          + "This reference is specified in this form:\n"
          + "- name@hash - Identifies the 'hash' commit on the named branch.\n"
          + "\n"
          + "The 'hash' commit must be reachable from the current HEAD of the branch.\n"
          + "In this case 'hash' indicates the state of contents known to the client and serves to ensure that the "
          + "operation is performed on the contents that the client expects.\n"
          + "This hash can point to a commit in the middle of the change history, but it should be as recent as "
          + "possible.\n";

  String REF_NAME_DESCRIPTION = "A reference name.\n\n" + REF_NAME_MESSAGE + "\n";

  String REF_GET_PARAMETER_DESCRIPTION =
      "Specifies a reference to a particular commit history branch or tag.\n"
          + "\n"
          + "This reference can be specification in these forms:\n"
          + "- \\- (literal minus character) - identifies the default branch.\n"
          + "- name - Identifies the named branch or tag.\n";

  String REF_PARAMETER_DESCRIPTION =
      "A reference to a particular version of the contents tree (a point in history).\n"
          + "\n"
          + "Reference representations consist of:\n"
          + "- The reference name. '-' means the default branch name.\n"
          + "- A commit hash prefixed with '@'.\n"
          + "- A relative commit specification. '~N' means the N-th predecessor commit, '*T' means the commit for "
          + "which the timestamp T (milliseconds since epoch or ISO-8601 instant) is valid, '^N' means the N-th parent"
          + "in a commit (N=2 is the merge parent).\n"
          + "\n"
          + "If neither the reference name or the default branch name placeholder '-' is specified, "
          + "the reference type 'DETACHED' will be assumed."
          + "\n"
          + "If no commit hash is specified, the HEAD of the specified named reference will be used."
          + "\n"
          + "An empty reference parameter is not valid.\n"
          + "\n"
          + "This reference can be specified in these forms:\n"
          + "- \\- (literal minus character) - identifies the HEAD of the default branch.\n"
          + "- name - Identifies the HEAD commit of a branch or tag.\n"
          + "- name@hash - Identifies the 'hash' commit on a branch or tag.\n"
          + "- @hash - Identifies the 'hash' commit in an unspecified branch or tag.\n"
          + "- -~3 - The 3rd predecessor commit from the HEAD of the default branch.\n"
          + "- name~3 - The 3rd predecessor commit from the HEAD of a branch or tag.\n"
          + "- @hash~3 - The 3rd predecessor commit of the 'hash' commit.\n"
          + "- name@hash^2 - The merge parent of the 'hash' commit of a branch or tag.\n"
          + "- @hash^2 - The merge parent of the 'hash' commit.\n"
          + "- -*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of the default branch for the given ISO-8601 timestamp.\n"
          + "- name*2021-04-07T14:42:25.534748Z - The predecessor commit closest to the HEAD of a branch or tag valid for the given ISO-8601 timestamp.\n"
          + "- name*1685185847230 - The predecessor commit closest to the HEAD of a branch or tag valid for the given timestamp in milliseconds since epoch.\n"
          + "\n"
          + "If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. "
          + "If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash "
          + "without claiming its reachability from any live HEAD). Using references of the last form may have "
          + "authorization implications when compared to an equivalent reference of the former forms.\n"
          + "\n"
          + "An empty reference parameter is invalid.\n"
          + "\n"
          + FULL_REF_INFO;

  String WITH_DOC_PARAMETER_DESCRIPTION =
      "Whether to return the documentation, if it exists. Default is to not return the documentation.";

  String FOR_WRITE_PARAMETER_DESCRIPTION =
      "If set to 'true', access control checks will check for write/create privilege in addition to read privileges.";

  String CHECKED_REF_DESCRIPTION =
      "Specifies a named branch or tag reference with its expected HEAD 'hash' value.\n"
          + "\n"
          + "For example:\n"
          + "- name@hash - Identifies the 'hash' commit on a branch or tag.\n"
          + "\n"
          + "The specified 'hash' must be the value of the current HEAD of the branch or tag known by the client. It will be used to validate that "
          + "at execution time the reference points to the same hash that the caller expected "
          + "when the operation arguments were constructed.\n";

  String CHECKED_REF_INFO =
      "The 'ref' parameter may contain a hash qualifier. That hash as well as the optional "
          + "'type' parameter may be used to ensure the operation is performed on the same object that the user "
          + "expects.\n";

  String KEY_ELEMENTS_DESCRIPTION =
      "Content key and namespace components are separated by the dot (`.`) character.\n"
          + "The components itself must be escaped using the rules described in "
          + "[NESSIE-SPEC-2.0.md in the repository](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md).";

  String KEY_PARAMETER_DESCRIPTION = "The key to a content object.\n\n" + KEY_ELEMENTS_DESCRIPTION;

  String REQUESTED_KEY_PARAMETER_DESCRIPTION =
      "Restrict the result to one or more keys.\n\n"
          + "Can be combined with min/max-key and prefix-key parameters, however both predicates must match. "
          + "This means that keys specified via this parameter that do not match a given min/max-key or prefix-key will not be returned.\n\n"
          + KEY_ELEMENTS_DESCRIPTION;

  String KEY_MIN_PARAMETER_DESCRIPTION =
      "The lower bound of the content key range to retrieve (inclusive). "
          + "The content keys of all returned entries will be greater than or equal to the min-value. "
          + "Content-keys are compared as a 'whole', unlike prefix-keys.\n\n"
          + KEY_ELEMENTS_DESCRIPTION;

  String KEY_MAX_PARAMETER_DESCRIPTION =
      "The upper bound of the content key range to retrieve (inclusive). "
          + "The content keys of all returned entries will be less than or equal to the max-value. "
          + "Content-keys are compared as a 'whole', unlike prefix-keys.\n\n"
          + KEY_ELEMENTS_DESCRIPTION;

  String KEY_PREFIX_PARAMETER_DESCRIPTION =
      "The content key prefix to retrieve (inclusive). "
          + "A content key matches a given prefix, a content key's elements starts with all elements of the prefix-key. "
          + "Key prefixes exactly match key-element boundaries.\n\n"
          + "Must not be combined with min/max-key parameters.\n\n"
          + KEY_ELEMENTS_DESCRIPTION;

  String DEFAULT_KEY_MERGE_MODE_DESCRIPTION =
      "The default merge mode. If not set, `NORMAL` is assumed.\n"
          + "\n"
          + "This settings applies to key thaWhen set to 'true' instructs the server to validate the request\n"
          + "        but to avoid committing any changes.t are not explicitly mentioned in the `keyMergeModes` property.\n";

  String KEY_MERGE_MODES_DESCRIPTION =
      "Specific merge behaviour requests by content key.\n"
          + "\n"
          + "The default is set by the `defaultKeyMergeMode` parameter.\n";

  String FROM_REF_NAME_DESCRIPTION =
      "The name of the reference that contains the 'source' commits for the requested merge or transplant operation.\n";

  String DRY_RUN_DESCRIPTION =
      "When set to 'true' instructs the server to validate the request but to avoid committing any changes.\n";

  String FETCH_ADDITION_INFO_DESCRIPTION = "Whether to provide optional response data.\n";

  String RETURN_CONFLICTS_AS_RESULT_DESCRIPTION =
      "When set to 'true' instructs the server to produce normal (non-error) responses in case a conflict is "
          + "detected and report conflict details in the response payload.";
}
