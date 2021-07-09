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

import { exists, mapValues } from '../runtime';
import {
    Contents,
    ContentsFromJSON,
    ContentsFromJSONTyped,
    ContentsToJSON,
    ContentsKey,
    ContentsKeyFromJSON,
    ContentsKeyFromJSONTyped,
    ContentsKeyToJSON,
} from './';

/**
 * 
 * @export
 * @interface ContentsWithKey
 */
export interface ContentsWithKey {
    /**
     * 
     * @type {Contents}
     * @memberof ContentsWithKey
     */
    contents: Contents;
    /**
     * 
     * @type {ContentsKey}
     * @memberof ContentsWithKey
     */
    key: ContentsKey;
}

export function ContentsWithKeyFromJSON(json: any): ContentsWithKey {
    return ContentsWithKeyFromJSONTyped(json, false);
}

export function ContentsWithKeyFromJSONTyped(json: any, ignoreDiscriminator: boolean): ContentsWithKey {
    if ((json === undefined) || (json === null)) {
        return json;
    }
    return {
        
        'contents': ContentsFromJSON(json['contents']),
        'key': ContentsKeyFromJSON(json['key']),
    };
}

export function ContentsWithKeyToJSON(value?: ContentsWithKey | null): any {
    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }
    return {
        
        'contents': ContentsToJSON(value.contents),
        'key': ContentsKeyToJSON(value.key),
    };
}


