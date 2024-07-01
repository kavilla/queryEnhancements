/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { ALL_NAMESPACES_STRING } from 'src/core/server/saved_objects/service/lib/utils';
import {
  ISavedObjectTypeRegistry,
  SavedObject,
  SavedObjectsErrorHelpers,
  SavedObjectsRawDoc,
  SavedObjectsUtils,
} from 'src/core/server';
import { DecoratedError } from 'src/core/server/saved_objects/service/lib/errors';
import { opensearchtypes } from '@opensearch-project/opensearch/.';
import { SavedObjectsRawDocSource } from 'src/core/server/saved_objects/serialization';
import { decodeRequestVersion, encodeHitVersion } from 'src/core/server/saved_objects/version';
import { GetResponseFound } from '../types';

export function getBulkOperationError(
  error: { type: string; reason?: string },
  type: string,
  id: string
) {
  switch (error.type) {
    case 'version_conflict_engine_exception':
      return errorContent(SavedObjectsErrorHelpers.createConflictError(type, id));
    case 'document_missing_exception':
      return errorContent(SavedObjectsErrorHelpers.createGenericNotFoundError(type, id));
    default:
      return {
        message: error.reason || JSON.stringify(error),
      };
  }
}

/**
 * Returns an object with the expected version properties. This facilitates OpenSearch's Optimistic Concurrency Control.
 *
 * @param version Optional version specified by the consumer.
 * @param document Optional existing document that was obtained in a preflight operation.
 */
export function getExpectedVersionProperties(version?: string, document?: SavedObjectsRawDoc) {
  if (version) {
    return decodeRequestVersion(version);
  } else if (document) {
    return {
      if_seq_no: document._seq_no,
      if_primary_term: document._primary_term,
    };
  }
  return {};
}

/**
 * Returns a string array of namespaces for a given saved object. If the saved object is undefined, the result is an array that contains the
 * current namespace. Value may be undefined if an existing saved object has no namespaces attribute; this should not happen in normal
 * operations, but it is possible if the OpenSearch document is manually modified.
 *
 * @param namespace The current namespace.
 * @param document Optional existing saved object that was obtained in a preflight operation.
 */
export function getSavedObjectNamespaces(
  namespace?: string,
  document?: SavedObjectsRawDoc
): string[] | undefined {
  if (document) {
    return document._source?.namespaces;
  }
  return [SavedObjectsUtils.namespaceIdToString(namespace)];
}

/**
 * Gets a saved object from a raw OpenSearch document.
 *
 * @param registry Registry which holds the registered saved object types information.
 * @param type The type of the saved object.
 * @param id The ID of the saved object.
 * @param doc Doc contains _source and optional _seq_no and _primary_term.
 */
export function getSavedObjectFromSource<T>(
  registry: ISavedObjectTypeRegistry,
  type: string,
  id: string,
  doc: { _seq_no?: number; _primary_term?: number; _source: SavedObjectsRawDocSource }
): SavedObject<T> {
  const { originId, updated_at: updatedAt, workspaces, permissions } = doc._source;

  let namespaces: string[] = [];
  if (!registry.isNamespaceAgnostic(type)) {
    namespaces = doc._source.namespaces ?? [
      SavedObjectsUtils.namespaceIdToString(doc._source.namespace),
    ];
  }

  return {
    id,
    type,
    namespaces,
    ...(originId && { originId }),
    ...(updatedAt && { updated_at: updatedAt }),
    ...(workspaces && { workspaces }),
    version: encodeHitVersion(doc),
    attributes: doc._source[type],
    references: doc._source.references || [],
    migrationVersion: doc._source.migrationVersion,
    ...(permissions && { permissions }),
  };
}

/**
 * Ensure that a namespace is always in its namespace ID representation.
 * This allows `'default'` to be used interchangeably with `undefined`.
 */
export const normalizeNamespace = (namespace?: string) => {
  if (namespace === ALL_NAMESPACES_STRING) {
    throw SavedObjectsErrorHelpers.createBadRequestError('"options.namespace" cannot be "*"');
  } else if (namespace === undefined) {
    return namespace;
  } else {
    return SavedObjectsUtils.namespaceStringToId(namespace);
  }
};

/**
 * Extracts the contents of a decorated error to return the attributes for bulk operations.
 */
export const errorContent = (error: DecoratedError) => error.output.payload;

export const unique = (array: string[]) => [...new Set(array)];

export const isFoundGetResponse = <TDocument = unknown>(
  doc: opensearchtypes.GetResponse<TDocument>
): doc is GetResponseFound<TDocument> => doc.found;
