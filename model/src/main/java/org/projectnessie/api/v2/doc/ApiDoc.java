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

  String BRANCH_DESCRIPTION =
      "A reference to a particular version of the contents tree (a point in history) on a branch.\n"
          + "This reference can be specification in these forms:\n"
          + "- \\- (literal minus character) - Identifies the HEAD of the default branch \n"
          + "- name - Identifies the HEAD commit on the named branch\n"
          + "- name@hash - Identifies the 'hash' commit on the named branch.\n"
          + "\n"
          + "If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch.\n"
          + "In this case 'hash' indicates the state of contents that should be used for validating incoming changes\n"
          + "(commits / merges / transplants).\n"
          + "\n"
          + "Note that using the simple 'name' form will effectively disable content conflict checks and is "
          + "generally discouraged.\n"
          + "\n"
          + FULL_REF_INFO;

  String REF_NAME_DESCRIPTION = "A reference name.\n\n" + REF_NAME_MESSAGE + "\n";

  String REF_PARAMETER_DESCRIPTION =
      "A reference to a particular version of the contents tree (a point in history).\n"
          + "\n"
          + "This reference can be specification in these forms:\n"
          + "- \\- (literal minus character) - identifies the HEAD of the default branch.\n"
          + "- name - Identifies the HEAD commit of a branch or tag.\n"
          + "- name@hash - Identifies the 'hash' commit on a branch or tag.\n"
          + "- @hash - Identifies the 'hash' commit in an unspecified branch or tag.\n"
          + "\n"
          + "If both 'name' and 'hash' are given, 'hash' must be reachable from the current HEAD of the branch or tag. "
          + "If 'name' is omitted, the reference will be of type 'DETACHED' (referencing a specific commit hash "
          + "without claiming its reachability from any live HEAD). Using references of the last form may have "
          + "authorization implications when compared to an equivalent reference of the former forms.\n"
          + "\n"
          + FULL_REF_INFO;

  String CHECKED_REF_DESCRIPTION =
      "Specifies a named branch or tag reference.\n"
          + "\n"
          + "A named reference can be specification in these forms:\n"
          + "- \\- (literal minus character) - Identifies the default branch.\n"
          + "- name - Identifies a branch or tag without a concrete HEAD 'hash' value.\n"
          + "- name@hash - Identifies the 'hash' commit on a branch or tag.\n"
          + "\n"
          + "If both 'name' and 'hash' are given, 'hash' must be the current HEAD of the branch or tag. It will be "
          + "used to validate that at execution time the reference points to the same hash that the caller expected "
          + "when the operation arguments were constructed.\n"
          + "\n"
          + "Not specifying the 'hash' value relaxes server-side checks and can lead to unexpected side effects if "
          + "multiple changes to the same reference are executed concurrently. It is recommended to always specify the "
          + "'hash' value when assigning or deleting a reference.\n";

  String CHECKED_REF_INFO =
      "The 'ref' parameter may contain a hash qualifier. That hash as well as the optional "
          + "'type' parameter may be used to ensure the operation is performed on the same object that the user "
          + "expects.\n";

  String KEY_PARAMETER_DESCRIPTION =
      "The key to a content object.\n"
          + "\n"
          + "Key components (namespaces) are separated by the dot ('.') character. Dot ('.') characters that are not "
          + "Nessie namespace separators must be encoded as the 'group separator' ASCII character (0x1D).\n";

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
