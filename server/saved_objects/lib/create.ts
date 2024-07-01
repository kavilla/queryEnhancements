/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { SavedObjectsCreateOptions, SavedObject } from 'opensearch-dashboards/public';
import { SavedObjectsErrorHelpers, SavedObjectSanitizedDoc } from 'src/core/server';
import { decodeRequestVersion } from 'src/core/server/saved_objects/version';
import { OPENSEARCH_API } from '../../../common';
import { normalizeNamespace } from '../../utils';
import { getSavedObjectNamespaces } from '../../utils/saved_objects';

export async function createSavedObject<T = unknown>(
  client: any, // Adjust the type accordingly
  allowedTypes: string[],
  registry: any, // Adjust the type accordingly
  serializer: any, // Adjust the type accordingly
  type: string,
  attributes: T,
  options: SavedObjectsCreateOptions = {}
): Promise<SavedObject<T>> {
  const {
    id,
    migrationVersion,
    overwrite = false,
    references = [],
    originId,
    initialNamespaces,
    version,
  } = options;
  const namespace = normalizeNamespace(options.namespace);

  if (!allowedTypes.includes(type)) {
    throw SavedObjectsErrorHelpers.createUnsupportedTypeError(type);
  }

  let savedObjectNamespace;
  let savedObjectNamespaces: string[] | undefined;

  if (registry.isSingleNamespace(type) && namespace) {
    savedObjectNamespace = namespace;
  } else if (registry.isMultiNamespace(type)) {
    if (id && overwrite) {
      // we will overwrite a multi-namespace saved object if it exists; if that happens, ensure we preserve its included namespaces
      // note: this check throws an error if the object is found but does not exist in this namespace
      const existingNamespaces = await preflightGetNamespaces(registry, type, id, namespace);
      savedObjectNamespaces = initialNamespaces || existingNamespaces;
    } else {
      savedObjectNamespaces = initialNamespaces || getSavedObjectNamespaces(namespace);
    }
  }

  const raw = serializer.savedObjectToRaw({
    id,
    type,
    ...(savedObjectNamespace && { namespace: savedObjectNamespace }),
    ...(savedObjectNamespaces && { namespaces: savedObjectNamespaces }),
    originId,
    attributes,
    migrationVersion,
    updated_at: getCurrentTime(),
    ...(Array.isArray(references) && { references }),
  } as SavedObjectSanitizedDoc);

  const requestParams = {
    body: {
      name: raw._source[type].title,
      connector: raw._source[type].dataSourceEngineType,
      properties: {
        // TODO: add all the auth stuff
        ...raw._source[type],
      },
    },
    ...(overwrite && version ? decodeRequestVersion(version) : {}),
  };

  const { body } =
    id && overwrite
      ? await client.asScoped().callAsInternalUser(OPENSEARCH_API.DATA_CONNECTIONS, requestParams)
      : await client.asScoped().callAsInternalUser(OPENSEARCH_API.DATA_CONNECTIONS, requestParams);

  return rawToSavedObject<T>({
    ...raw,
    ...body,
  });
}
