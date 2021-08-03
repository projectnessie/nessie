/* tslint:disable */
/* eslint-disable */
/**
 * Nessie API
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.8.0
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


import * as runtime from '../runtime';
import {
    Branch,
    BranchFromJSON,
    BranchToJSON,
    Contents,
    ContentsFromJSON,
    ContentsToJSON,
    ContentsKey,
    ContentsKeyFromJSON,
    ContentsKeyToJSON,
    EntriesResponse,
    EntriesResponseFromJSON,
    EntriesResponseToJSON,
    LogResponse,
    LogResponseFromJSON,
    LogResponseToJSON,
    Merge,
    MergeFromJSON,
    MergeToJSON,
    MultiGetContentsRequest,
    MultiGetContentsRequestFromJSON,
    MultiGetContentsRequestToJSON,
    MultiGetContentsResponse,
    MultiGetContentsResponseFromJSON,
    MultiGetContentsResponseToJSON,
    NessieConfiguration,
    NessieConfigurationFromJSON,
    NessieConfigurationToJSON,
    Operations,
    OperationsFromJSON,
    OperationsToJSON,
    Reference,
    ReferenceFromJSON,
    ReferenceToJSON,
    Transplant,
    TransplantFromJSON,
    TransplantToJSON,
} from '../models';

export interface AssignBranchRequest {
    branchName: string;
    expectedHash: string;
    reference?: Reference;
}

export interface AssignTagRequest {
    tagName: string;
    expectedHash: string;
    reference?: Reference;
}

export interface CommitMultipleOperationsRequest {
    branchName: string;
    expectedHash: string;
    operations?: Operations;
}

export interface CreateReferenceRequest {
    sourceRef?: string;
    reference?: Reference;
}

export interface DeleteBranchRequest {
    branchName: string;
    expectedHash: string;
}

export interface DeleteTagRequest {
    tagName: string;
    expectedHash?: string;
}

export interface GetCommitLogRequest {
    ref: string;
    endHash?: string;
    max?: number;
    pageToken?: string;
    queryExpression?: string;
    startHash?: string;
}

export interface GetContentsRequest {
    key: ContentsKey;
    hashOnRef?: string;
    ref?: string;
}

export interface GetEntriesRequest {
    ref: string;
    hashOnRef?: string;
    max?: number;
    pageToken?: string;
    queryExpression?: string;
}

export interface GetMultipleContentsRequest {
    hashOnRef?: string;
    ref?: string;
    multiGetContentsRequest?: MultiGetContentsRequest;
}

export interface GetReferenceByNameRequest {
    ref: string;
}

export interface MergeRefIntoBranchRequest {
    branchName: string;
    expectedHash: string;
    merge?: Merge;
}

export interface TransplantCommitsIntoBranchRequest {
    branchName: string;
    expectedHash: string;
    message?: string;
    transplant?: Transplant;
}

/**
 * 
 */
export class DefaultApi extends runtime.BaseAPI {

    /**
     * Set a branch to a specific hash
     */
    async assignBranchRaw(requestParameters: AssignBranchRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.branchName === null || requestParameters.branchName === undefined) {
            throw new runtime.RequiredError('branchName','Required parameter requestParameters.branchName was null or undefined when calling assignBranch.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling assignBranch.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/branch/{branchName}`.replace(`{${"branchName"}}`, encodeURIComponent(String(requestParameters.branchName))),
            method: 'PUT',
            headers: headerParameters,
            query: queryParameters,
            body: ReferenceToJSON(requestParameters.reference),
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * Set a branch to a specific hash
     */
    async assignBranch(requestParameters: AssignBranchRequest): Promise<void> {
        await this.assignBranchRaw(requestParameters);
    }

    /**
     * Set a tag to a specific hash
     */
    async assignTagRaw(requestParameters: AssignTagRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.tagName === null || requestParameters.tagName === undefined) {
            throw new runtime.RequiredError('tagName','Required parameter requestParameters.tagName was null or undefined when calling assignTag.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling assignTag.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/tag/{tagName}`.replace(`{${"tagName"}}`, encodeURIComponent(String(requestParameters.tagName))),
            method: 'PUT',
            headers: headerParameters,
            query: queryParameters,
            body: ReferenceToJSON(requestParameters.reference),
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * Set a tag to a specific hash
     */
    async assignTag(requestParameters: AssignTagRequest): Promise<void> {
        await this.assignTagRaw(requestParameters);
    }

    /**
     * Commit multiple operations against the given branch expecting that branch to have the given hash as its latest commit. The hash in the successful response contains the hash of the commit that contains the operations of the invocation.
     */
    async commitMultipleOperationsRaw(requestParameters: CommitMultipleOperationsRequest): Promise<runtime.ApiResponse<Branch>> {
        if (requestParameters.branchName === null || requestParameters.branchName === undefined) {
            throw new runtime.RequiredError('branchName','Required parameter requestParameters.branchName was null or undefined when calling commitMultipleOperations.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling commitMultipleOperations.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/branch/{branchName}/commit`.replace(`{${"branchName"}}`, encodeURIComponent(String(requestParameters.branchName))),
            method: 'POST',
            headers: headerParameters,
            query: queryParameters,
            body: OperationsToJSON(requestParameters.operations),
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => BranchFromJSON(jsonValue));
    }

    /**
     * Commit multiple operations against the given branch expecting that branch to have the given hash as its latest commit. The hash in the successful response contains the hash of the commit that contains the operations of the invocation.
     */
    async commitMultipleOperations(requestParameters: CommitMultipleOperationsRequest): Promise<Branch> {
        const response = await this.commitMultipleOperationsRaw(requestParameters);
        return await response.value();
    }

    /**
     * The type of \'refObj\', which can be either a \'Branch\' or \'Tag\', determines the type of the reference to be created.  \'Reference.name\' defines the the name of the reference to be created,\'Reference.hash\' is the hash of the created reference, the HEAD of the created reference. \'sourceRef\' is the name of the reference which contains \'Reference.hash\', and must be present if \'Reference.hash\' is present.  Specifying no \'Reference.hash\' means that the new reference will be created \"at the beginning of time\".
     * Create a new reference
     */
    async createReferenceRaw(requestParameters: CreateReferenceRequest): Promise<runtime.ApiResponse<Reference>> {
        const queryParameters: any = {};

        if (requestParameters.sourceRef !== undefined) {
            queryParameters['sourceRef'] = requestParameters.sourceRef;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/tree`,
            method: 'POST',
            headers: headerParameters,
            query: queryParameters,
            body: ReferenceToJSON(requestParameters.reference),
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => ReferenceFromJSON(jsonValue));
    }

    /**
     * The type of \'refObj\', which can be either a \'Branch\' or \'Tag\', determines the type of the reference to be created.  \'Reference.name\' defines the the name of the reference to be created,\'Reference.hash\' is the hash of the created reference, the HEAD of the created reference. \'sourceRef\' is the name of the reference which contains \'Reference.hash\', and must be present if \'Reference.hash\' is present.  Specifying no \'Reference.hash\' means that the new reference will be created \"at the beginning of time\".
     * Create a new reference
     */
    async createReference(requestParameters: CreateReferenceRequest): Promise<Reference> {
        const response = await this.createReferenceRaw(requestParameters);
        return await response.value();
    }

    /**
     * Delete a branch endpoint
     */
    async deleteBranchRaw(requestParameters: DeleteBranchRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.branchName === null || requestParameters.branchName === undefined) {
            throw new runtime.RequiredError('branchName','Required parameter requestParameters.branchName was null or undefined when calling deleteBranch.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling deleteBranch.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/branch/{branchName}`.replace(`{${"branchName"}}`, encodeURIComponent(String(requestParameters.branchName))),
            method: 'DELETE',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * Delete a branch endpoint
     */
    async deleteBranch(requestParameters: DeleteBranchRequest): Promise<void> {
        await this.deleteBranchRaw(requestParameters);
    }

    /**
     * Delete a tag
     */
    async deleteTagRaw(requestParameters: DeleteTagRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.tagName === null || requestParameters.tagName === undefined) {
            throw new runtime.RequiredError('tagName','Required parameter requestParameters.tagName was null or undefined when calling deleteTag.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/tag/{tagName}`.replace(`{${"tagName"}}`, encodeURIComponent(String(requestParameters.tagName))),
            method: 'DELETE',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * Delete a tag
     */
    async deleteTag(requestParameters: DeleteTagRequest): Promise<void> {
        await this.deleteTagRaw(requestParameters);
    }

    /**
     * Get all references
     */
    async getAllReferencesRaw(): Promise<runtime.ApiResponse<Array<Reference>>> {
        const queryParameters: any = {};

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees`,
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => jsonValue.map(ReferenceFromJSON));
    }

    /**
     * Get all references
     */
    async getAllReferences(): Promise<Array<Reference>> {
        const response = await this.getAllReferencesRaw();
        return await response.value();
    }

    /**
     * Retrieve the commit log for a ref, potentially truncated by the backend.  Retrieves up to \'maxRecords\' commit-log-entries starting at the HEAD of the given named reference (tag or branch) or the given hash. The backend may respect the given \'max\' records hint, but return less or more entries. Backends may also cap the returned entries at a hard-coded limit, the default REST server implementation has such a hard-coded limit.  To implement paging, check \'hasMore\' in the response and, if \'true\', pass the value returned as \'token\' in the next invocation as the \'pageToken\' parameter.  The content and meaning of the returned \'token\' is \"private\" to the implementation,treat is as an opaque value.  It is wrong to assume that invoking this method with a very high \'maxRecords\' value will return all commit log entries.  The \'query_expression\' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL). An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. 
     * Get commit log for a reference
     */
    async getCommitLogRaw(requestParameters: GetCommitLogRequest): Promise<runtime.ApiResponse<LogResponse>> {
        if (requestParameters.ref === null || requestParameters.ref === undefined) {
            throw new runtime.RequiredError('ref','Required parameter requestParameters.ref was null or undefined when calling getCommitLog.');
        }

        const queryParameters: any = {};

        if (requestParameters.endHash !== undefined) {
            queryParameters['endHash'] = requestParameters.endHash;
        }

        if (requestParameters.max !== undefined) {
            queryParameters['max'] = requestParameters.max;
        }

        if (requestParameters.pageToken !== undefined) {
            queryParameters['pageToken'] = requestParameters.pageToken;
        }

        if (requestParameters.queryExpression !== undefined) {
            queryParameters['query_expression'] = requestParameters.queryExpression;
        }

        if (requestParameters.startHash !== undefined) {
            queryParameters['startHash'] = requestParameters.startHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/tree/{ref}/log`.replace(`{${"ref"}}`, encodeURIComponent(String(requestParameters.ref))),
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => LogResponseFromJSON(jsonValue));
    }

    /**
     * Retrieve the commit log for a ref, potentially truncated by the backend.  Retrieves up to \'maxRecords\' commit-log-entries starting at the HEAD of the given named reference (tag or branch) or the given hash. The backend may respect the given \'max\' records hint, but return less or more entries. Backends may also cap the returned entries at a hard-coded limit, the default REST server implementation has such a hard-coded limit.  To implement paging, check \'hasMore\' in the response and, if \'true\', pass the value returned as \'token\' in the next invocation as the \'pageToken\' parameter.  The content and meaning of the returned \'token\' is \"private\" to the implementation,treat is as an opaque value.  It is wrong to assume that invoking this method with a very high \'maxRecords\' value will return all commit log entries.  The \'query_expression\' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL). An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. 
     * Get commit log for a reference
     */
    async getCommitLog(requestParameters: GetCommitLogRequest): Promise<LogResponse> {
        const response = await this.getCommitLogRaw(requestParameters);
        return await response.value();
    }

    /**
     * List all configuration settings
     */
    async getConfigRaw(): Promise<runtime.ApiResponse<NessieConfiguration>> {
        const queryParameters: any = {};

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/config`,
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => NessieConfigurationFromJSON(jsonValue));
    }

    /**
     * List all configuration settings
     */
    async getConfig(): Promise<NessieConfiguration> {
        const response = await this.getConfigRaw();
        return await response.value();
    }

    /**
     * This operation returns a consistent view for a contents-key in a branch or tag.  Nessie may return a \'Contents\' object, that is updated compared to the value that has been passed when the \'Contents\' object has been committed, to reflect a more recent, but semantically equal state.
     * Get object content associated with a key.
     */
    async getContentsRaw(requestParameters: GetContentsRequest): Promise<runtime.ApiResponse<Contents>> {
        if (requestParameters.key === null || requestParameters.key === undefined) {
            throw new runtime.RequiredError('key','Required parameter requestParameters.key was null or undefined when calling getContents.');
        }

        const queryParameters: any = {};

        if (requestParameters.hashOnRef !== undefined) {
            queryParameters['hashOnRef'] = requestParameters.hashOnRef;
        }

        if (requestParameters.ref !== undefined) {
            queryParameters['ref'] = requestParameters.ref;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/contents/{key}`.replace(`{${"key"}}`, encodeURIComponent(String(requestParameters.key))),
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => ContentsFromJSON(jsonValue));
    }

    /**
     * This operation returns a consistent view for a contents-key in a branch or tag.  Nessie may return a \'Contents\' object, that is updated compared to the value that has been passed when the \'Contents\' object has been committed, to reflect a more recent, but semantically equal state.
     * Get object content associated with a key.
     */
    async getContents(requestParameters: GetContentsRequest): Promise<Contents> {
        const response = await this.getContentsRaw(requestParameters);
        return await response.value();
    }

    /**
     * Get default branch for commits and reads
     */
    async getDefaultBranchRaw(): Promise<runtime.ApiResponse<Branch>> {
        const queryParameters: any = {};

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/tree`,
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => BranchFromJSON(jsonValue));
    }

    /**
     * Get default branch for commits and reads
     */
    async getDefaultBranch(): Promise<Branch> {
        const response = await this.getDefaultBranchRaw();
        return await response.value();
    }

    /**
     * Retrieves objects for a ref, potentially truncated by the backend.  Retrieves up to \'maxRecords\' entries for the given named reference (tag or branch) or the given hash. The backend may respect the given \'max\' records hint, but return less or more entries. Backends may also cap the returned entries at a hard-coded limit, the default REST server implementation has such a hard-coded limit.  To implement paging, check \'hasMore\' in the response and, if \'true\', pass the value returned as \'token\' in the next invocation as the \'pageToken\' parameter.  The content and meaning of the returned \'token\' is \"private\" to the implementation,treat is as an opaque value.  It is wrong to assume that invoking this method with a very high \'maxRecords\' value will return all commit log entries.  The \'query_expression\' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL). An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. 
     * Fetch all entries for a given reference
     */
    async getEntriesRaw(requestParameters: GetEntriesRequest): Promise<runtime.ApiResponse<EntriesResponse>> {
        if (requestParameters.ref === null || requestParameters.ref === undefined) {
            throw new runtime.RequiredError('ref','Required parameter requestParameters.ref was null or undefined when calling getEntries.');
        }

        const queryParameters: any = {};

        if (requestParameters.hashOnRef !== undefined) {
            queryParameters['hashOnRef'] = requestParameters.hashOnRef;
        }

        if (requestParameters.max !== undefined) {
            queryParameters['max'] = requestParameters.max;
        }

        if (requestParameters.pageToken !== undefined) {
            queryParameters['pageToken'] = requestParameters.pageToken;
        }

        if (requestParameters.queryExpression !== undefined) {
            queryParameters['query_expression'] = requestParameters.queryExpression;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/tree/{ref}/entries`.replace(`{${"ref"}}`, encodeURIComponent(String(requestParameters.ref))),
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => EntriesResponseFromJSON(jsonValue));
    }

    /**
     * Retrieves objects for a ref, potentially truncated by the backend.  Retrieves up to \'maxRecords\' entries for the given named reference (tag or branch) or the given hash. The backend may respect the given \'max\' records hint, but return less or more entries. Backends may also cap the returned entries at a hard-coded limit, the default REST server implementation has such a hard-coded limit.  To implement paging, check \'hasMore\' in the response and, if \'true\', pass the value returned as \'token\' in the next invocation as the \'pageToken\' parameter.  The content and meaning of the returned \'token\' is \"private\" to the implementation,treat is as an opaque value.  It is wrong to assume that invoking this method with a very high \'maxRecords\' value will return all commit log entries.  The \'query_expression\' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL). An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md. 
     * Fetch all entries for a given reference
     */
    async getEntries(requestParameters: GetEntriesRequest): Promise<EntriesResponse> {
        const response = await this.getEntriesRaw(requestParameters);
        return await response.value();
    }

    /**
     * Similar to \'getContents\', but takes multiple \'ContentKey\'s and returns a consistent view for these contents-keys in a branch or tag.  Nessie may return \'Contents\' objects, that are updated compared to the values that have been passed when the \'Contents\' objects have been committed, to reflect a more recent, but semantically equal state.
     * Get multiple objects\' content.
     */
    async getMultipleContentsRaw(requestParameters: GetMultipleContentsRequest): Promise<runtime.ApiResponse<MultiGetContentsResponse>> {
        const queryParameters: any = {};

        if (requestParameters.hashOnRef !== undefined) {
            queryParameters['hashOnRef'] = requestParameters.hashOnRef;
        }

        if (requestParameters.ref !== undefined) {
            queryParameters['ref'] = requestParameters.ref;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/contents`,
            method: 'POST',
            headers: headerParameters,
            query: queryParameters,
            body: MultiGetContentsRequestToJSON(requestParameters.multiGetContentsRequest),
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => MultiGetContentsResponseFromJSON(jsonValue));
    }

    /**
     * Similar to \'getContents\', but takes multiple \'ContentKey\'s and returns a consistent view for these contents-keys in a branch or tag.  Nessie may return \'Contents\' objects, that are updated compared to the values that have been passed when the \'Contents\' objects have been committed, to reflect a more recent, but semantically equal state.
     * Get multiple objects\' content.
     */
    async getMultipleContents(requestParameters: GetMultipleContentsRequest): Promise<MultiGetContentsResponse> {
        const response = await this.getMultipleContentsRaw(requestParameters);
        return await response.value();
    }

    /**
     * Fetch details of a reference
     */
    async getReferenceByNameRaw(requestParameters: GetReferenceByNameRequest): Promise<runtime.ApiResponse<Reference>> {
        if (requestParameters.ref === null || requestParameters.ref === undefined) {
            throw new runtime.RequiredError('ref','Required parameter requestParameters.ref was null or undefined when calling getReferenceByName.');
        }

        const queryParameters: any = {};

        const headerParameters: runtime.HTTPHeaders = {};

        const response = await this.request({
            path: `/trees/tree/{ref}`.replace(`{${"ref"}}`, encodeURIComponent(String(requestParameters.ref))),
            method: 'GET',
            headers: headerParameters,
            query: queryParameters,
        });

        return new runtime.JSONApiResponse(response, (jsonValue) => ReferenceFromJSON(jsonValue));
    }

    /**
     * Fetch details of a reference
     */
    async getReferenceByName(requestParameters: GetReferenceByNameRequest): Promise<Reference> {
        const response = await this.getReferenceByNameRaw(requestParameters);
        return await response.value();
    }

    /**
     * Merge items from an existing hash in \'mergeRef\' into the requested branch. The merge is always a rebase + fast-forward merge and is only completed if the rebase is conflict free. The set of commits added to the branch will be all of those until we arrive at a common ancestor. Depending on the underlying implementation, the number of commits allowed as part of this operation may be limited.
     * Merge commits from \'mergeRef\' onto \'branchName\'.
     */
    async mergeRefIntoBranchRaw(requestParameters: MergeRefIntoBranchRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.branchName === null || requestParameters.branchName === undefined) {
            throw new runtime.RequiredError('branchName','Required parameter requestParameters.branchName was null or undefined when calling mergeRefIntoBranch.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling mergeRefIntoBranch.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/branch/{branchName}/merge`.replace(`{${"branchName"}}`, encodeURIComponent(String(requestParameters.branchName))),
            method: 'POST',
            headers: headerParameters,
            query: queryParameters,
            body: MergeToJSON(requestParameters.merge),
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * Merge items from an existing hash in \'mergeRef\' into the requested branch. The merge is always a rebase + fast-forward merge and is only completed if the rebase is conflict free. The set of commits added to the branch will be all of those until we arrive at a common ancestor. Depending on the underlying implementation, the number of commits allowed as part of this operation may be limited.
     * Merge commits from \'mergeRef\' onto \'branchName\'.
     */
    async mergeRefIntoBranch(requestParameters: MergeRefIntoBranchRequest): Promise<void> {
        await this.mergeRefIntoBranchRaw(requestParameters);
    }

    /**
     * This is done as an atomic operation such that only the last of the sequence is ever visible to concurrent readers/writers. The sequence to transplant must be contiguous, in order and share a common ancestor with the target branch.
     * Transplant commits from \'transplant\' onto \'branchName\'
     */
    async transplantCommitsIntoBranchRaw(requestParameters: TransplantCommitsIntoBranchRequest): Promise<runtime.ApiResponse<void>> {
        if (requestParameters.branchName === null || requestParameters.branchName === undefined) {
            throw new runtime.RequiredError('branchName','Required parameter requestParameters.branchName was null or undefined when calling transplantCommitsIntoBranch.');
        }

        if (requestParameters.expectedHash === null || requestParameters.expectedHash === undefined) {
            throw new runtime.RequiredError('expectedHash','Required parameter requestParameters.expectedHash was null or undefined when calling transplantCommitsIntoBranch.');
        }

        const queryParameters: any = {};

        if (requestParameters.expectedHash !== undefined) {
            queryParameters['expectedHash'] = requestParameters.expectedHash;
        }

        if (requestParameters.message !== undefined) {
            queryParameters['message'] = requestParameters.message;
        }

        const headerParameters: runtime.HTTPHeaders = {};

        headerParameters['Content-Type'] = 'application/json';

        const response = await this.request({
            path: `/trees/branch/{branchName}/transplant`.replace(`{${"branchName"}}`, encodeURIComponent(String(requestParameters.branchName))),
            method: 'POST',
            headers: headerParameters,
            query: queryParameters,
            body: TransplantToJSON(requestParameters.transplant),
        });

        return new runtime.VoidApiResponse(response);
    }

    /**
     * This is done as an atomic operation such that only the last of the sequence is ever visible to concurrent readers/writers. The sequence to transplant must be contiguous, in order and share a common ancestor with the target branch.
     * Transplant commits from \'transplant\' onto \'branchName\'
     */
    async transplantCommitsIntoBranch(requestParameters: TransplantCommitsIntoBranchRequest): Promise<void> {
        await this.transplantCommitsIntoBranchRaw(requestParameters);
    }

}
