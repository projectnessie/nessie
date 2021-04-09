/* tslint:disable */
/* eslint-disable */
/**
 * Nessie API
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.5.0
 *
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

import { exists, mapValues } from '../runtime';
import {
    CommitMeta,
    CommitMetaFromJSON,
    CommitMetaFromJSONTyped,
    CommitMetaToJSON,
    Operation,
    OperationFromJSON,
    OperationFromJSONTyped,
    OperationToJSON,
} from './';

/**
 *
 * @export
 * @interface Operations
 */
export interface Operations {
    /**
     *
     * @type {CommitMeta}
     * @memberof Operations
     */
    commitMeta?: CommitMeta;
    /**
     *
     * @type {Array<Operation>}
     * @memberof Operations
     */
    operations?: Array<Operation>;
}

export function OperationsFromJSON(json: any): Operations {
    return OperationsFromJSONTyped(json, false);
}

export function OperationsFromJSONTyped(json: any, ignoreDiscriminator: boolean): Operations {
    if ((json === undefined) || (json === null)) {
        return json;
    }
    return {

        'commitMeta': !exists(json, 'commitMeta') ? undefined : CommitMetaFromJSON(json['commitMeta']),
        'operations': !exists(json, 'operations') ? undefined : ((json['operations'] as Array<any>).map(OperationFromJSON)),
    };
}

export function OperationsToJSON(value?: Operations | null): any {
    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }
    return {

        'commitMeta': CommitMetaToJSON(value.commitMeta),
        'operations': value.operations === undefined ? undefined : ((value.operations as Array<any>).map(OperationToJSON)),
    };
}
