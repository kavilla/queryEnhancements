/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { opensearchtypes } from '@opensearch-project/opensearch';
import { omit } from 'lodash';
import {
  DeleteDocumentResponse,
  ILegacyClusterClient,
  ISavedObjectTypeRegistry,
  MutatingOperationRefreshSetting,
  SavedObject,
  SavedObjectSanitizedDoc,
  SavedObjectTypeRegistry,
  SavedObjectsAddToNamespacesOptions,
  SavedObjectsAddToNamespacesResponse,
  SavedObjectsBaseOptions,
  SavedObjectsBulkCreateObject,
  SavedObjectsBulkGetObject,
  SavedObjectsBulkResponse,
  SavedObjectsBulkUpdateObject,
  SavedObjectsBulkUpdateOptions,
  SavedObjectsBulkUpdateResponse,
  SavedObjectsCheckConflictsObject,
  SavedObjectsCheckConflictsResponse,
  SavedObjectsCreateOptions,
  SavedObjectsDeleteByNamespaceOptions,
  SavedObjectsDeleteByWorkspaceOptions,
  SavedObjectsDeleteFromNamespacesOptions,
  SavedObjectsDeleteFromNamespacesResponse,
  SavedObjectsDeleteOptions,
  SavedObjectsErrorHelpers,
  SavedObjectsFindOptions,
  SavedObjectsFindResponse,
  SavedObjectsFindResult,
  SavedObjectsMigrationVersion,
  SavedObjectsRawDoc,
  SavedObjectsSerializer,
  SavedObjectsUpdateOptions,
  SavedObjectsUpdateResponse,
  SavedObjectsUtils,
} from 'src/core/server';
import { IndexMapping, getRootPropertiesObjects } from 'src/core/server/saved_objects/mappings';
import { IOpenSearchDashboardsMigrator } from 'src/core/server/saved_objects/migrations';
import { SavedObjectsRawDocSource } from 'src/core/server/saved_objects/serialization';
import { DecoratedError } from 'src/core/server/saved_objects/service/lib/errors';
import { validateConvertFilterToKueryNode } from 'src/core/server/saved_objects/service/lib/filter_utils';
import { includedFields } from 'src/core/server/saved_objects/service/lib/included_fields';
import { getSearchDsl } from 'src/core/server/saved_objects/service/lib/search_dsl';
import {
  ALL_NAMESPACES_STRING,
  FIND_DEFAULT_PAGE,
  FIND_DEFAULT_PER_PAGE,
} from 'src/core/server/saved_objects/service/lib/utils';
import {
  decodeRequestVersion,
  encodeHitVersion,
  encodeVersion,
} from 'src/core/server/saved_objects/version';
import uuid from 'uuid';
import { OPENSEARCH_API } from '../../common';
import {
  ExternalSavedObjectsIncrementCounterOptions,
  ExternalSavedObjectsRepositoryOptions,
  GetResponseFound,
  IExternalSavedObjectsRepository,
} from '../types';
import { errorContent, isFoundGetResponse, normalizeNamespace, unique } from '../utils';
import { getSavedObjectNamespaces } from '../utils/saved_objects';

// BEWARE: The SavedObjectClient depends on the implementation details of the SavedObjectsRepository
// so any breaking changes to this repository are considered breaking changes to the SavedObjectsClient.

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
type Left = { tag: 'Left'; error: Record<string, any> };
// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
type Right = { tag: 'Right'; value: Record<string, any> };
type Either = Left | Right;
const isLeft = (either: Either): either is Left => either.tag === 'Left';
const isRight = (either: Either): either is Right => either.tag === 'Right';

const DEFAULT_REFRESH_SETTING = 'wait_for';

export class ExternalSavedObjectsRepository {
  private _index: string;
  private _mappings: IndexMapping;
  private _registry: SavedObjectTypeRegistry;
  private _allowedTypes: string[];
  private readonly client: ILegacyClusterClient;
  private _serializer: SavedObjectsSerializer;

  public static createRepository(
    typeRegistry: SavedObjectTypeRegistry,
    indexName: string,
    client: ILegacyClusterClient,
    includedHiddenTypes: string[] = [],
    injectedConstructor: any = ExternalSavedObjectsRepository
  ): IExternalSavedObjectsRepository {
    const allTypes = typeRegistry.getAllTypes().map((t) => t.name);
    const serializer = new SavedObjectsSerializer(typeRegistry);
    const visibleTypes = allTypes.filter((type) => !typeRegistry.isHidden(type));

    const missingTypeMappings = includedHiddenTypes.filter((type) => !allTypes.includes(type));
    if (missingTypeMappings.length > 0) {
      throw new Error(
        `Missing mappings for saved objects types: '${missingTypeMappings.join(', ')}'`
      );
    }

    const allowedTypes = [...new Set(visibleTypes.concat(includedHiddenTypes))];

    return new injectedConstructor({
      index: indexName,
      typeRegistry,
      serializer,
      allowedTypes,
      client,
    });
  }

  constructor(options: ExternalSavedObjectsRepositoryOptions) {
    const { index, mappings, client, typeRegistry, serializer, allowedTypes = [] } = options;
    this._index = index;
    this._mappings = mappings;
    this._registry = typeRegistry;
    this.client = client;
    if (allowedTypes.length === 0) {
      throw new Error('Empty or missing types for saved object repository!');
    }
    this._allowedTypes = allowedTypes;
    this._serializer = serializer;
  }

  /**
   * Persists an object
   *
   * @param {string} type
   * @param {object} attributes
   * @param {object} [options={}]
   * @property {string} [options.id] - force id on creation, not recommended
   * @property {boolean} [options.overwrite=false]
   * @property {object} [options.migrationVersion=undefined]
   * @property {string} [options.namespace]
   * @property {array} [options.references=[]] - [{ name, type, id }]
   * @returns {promise} - { id, type, version, attributes }
   */
  async create<T = unknown>(
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

    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createUnsupportedTypeError(type);
    }

    let savedObjectNamespace;
    let savedObjectNamespaces: string[] | undefined;

    if (this._registry.isSingleNamespace(type) && namespace) {
      savedObjectNamespace = namespace;
    } else if (this._registry.isMultiNamespace(type)) {
      if (id && overwrite) {
        // we will overwrite a multi-namespace saved object if it exists; if that happens, ensure we preserve its included namespaces
        // note: this check throws an error if the object is found but does not exist in this namespace
        const existingNamespaces = await this.preflightGetNamespaces(type, id, namespace);
        savedObjectNamespaces = initialNamespaces || existingNamespaces;
      } else {
        savedObjectNamespaces = initialNamespaces || getSavedObjectNamespaces(namespace);
      }
    }

    const raw = this._serializer.savedObjectToRaw({
      id,
      type,
      ...(savedObjectNamespace && { namespace: savedObjectNamespace }),
      ...(savedObjectNamespaces && { namespaces: savedObjectNamespaces }),
      originId,
      attributes,
      migrationVersion,
      updated_at: this._getCurrentTime(),
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
        ? await this.client
            .asScoped()
            .callAsInternalUser(OPENSEARCH_API.DATA_CONNECTIONS, requestParams)
        : await this.client
            .asScoped()
            .callAsInternalUser(OPENSEARCH_API.DATA_CONNECTIONS, requestParams);

    return this._rawToSavedObject<T>({
      ...raw,
      ...body,
    });
  }

  async bulkCreate<T = unknown>(
    objects: Array<SavedObjectsBulkCreateObject<T>>,
    options: SavedObjectsCreateOptions = {}
  ): Promise<SavedObjectsBulkResponse<T>> {
    const namespace = normalizeNamespace(options.namespace);
    // ToDo: Do validation of objects as we do in OpenSearch.
    // For sake of POC, we are just inserting all object in a loop.
    const query = `INSERT INTO metadatastore(id, type, version, attributes, reference, migrationversion, namespaces, originid, updated_at, application_id) VALUES `;

    const expectedBulkResult = objects.map((object) => {
      // const refresh = options.refresh; // We don't need refresh for SQL operation.
      // ToDo: For now we are just storing version in table. Later we need to decide whether we want to use it for concurrency control or not.
      const raw = this.getSavedObjectRawDoc(
        object.type,
        object.attributes,
        object as SavedObjectsCreateOptions,
        namespace,
        []
      );

      const insertValuesExpr = `('${raw._id}', '${object.type}',
      '${object.version ?? ''}', '${JSON.stringify(raw._source).replace(/'/g, `''`)}',
      '${JSON.stringify(raw._source.references)}',
      '${JSON.stringify(raw._source.migrationVersion ?? {})}',
      ${raw._source.namespaces ? `ARRAY[${raw._source.namespaces}]` : `'{}'`},
      '${raw._source.originId ?? ''}', '${raw._source.updated_at}', '${
        this.applicationId ?? 'default'
      }')`;
      // ToDo: Decide if you want to keep raw._source or raw._source[type] in attributes field.
      // Refactor code to insert all rows in single transaction.
      this.postgresClient
        .query(`${query} ${insertValuesExpr}`)
        .then(() => {
          console.log('Saved object inserted in kibana table successfully.');
        })
        .catch((error: any) => {
          console.error(`error occurred for this query -> "${query} ${insertValuesExpr}"`);
          throw new Error(error);
        });
      const expectedResult = { rawMigratedDoc: raw };
      return { tag: 'Right' as 'Right', value: expectedResult };
    });

    return {
      saved_objects: expectedBulkResult.map((expectedResult) => {
        // When method == 'index' the bulkResponse doesn't include the indexed
        // _source so we return rawMigratedDoc but have to spread the latest
        // _seq_no and _primary_term values from the rawResponse.
        const { rawMigratedDoc } = expectedResult.value;
        return this._rawToSavedObject({
          ...rawMigratedDoc,
        });
      }),
    };
  }

  async checkConflicts(
    objects: SavedObjectsCheckConflictsObject[] = [],
    options: SavedObjectsBaseOptions = {}
  ): Promise<SavedObjectsCheckConflictsResponse> {
    console.log(`Inside PostgresRepository checkConflicts`);
    if (objects.length === 0) {
      return { errors: [] };
    }

    const namespace = normalizeNamespace(options.namespace);
    const errors: SavedObjectsCheckConflictsResponse['errors'] = [];
    const expectedBulkGetResults = objects.map((object) => {
      const { type, id } = object;

      if (!this._allowedTypes.includes(type)) {
        const error = {
          id,
          type,
          error: errorContent(SavedObjectsErrorHelpers.createUnsupportedTypeError(type)),
        };
        errors.push(error);
      }

      return {
        value: {
          type,
          id,
        },
      };
    });
    let results: any;
    await Promise.all(
      expectedBulkGetResults.map(async ({ value: { type, id } }) => {
        await this.postgresClient
          .query(
            `SELECT * FROM metadatastore where id='${this._serializer.generateRawId(
              namespace,
              type,
              id
            )}' and application_id='${this.applicationId ?? 'default'}'`
          )
          .then((res: any) => {
            results = res.rows[0];
            if (results && results.length > 0) {
              errors.push({
                id,
                type,
                error: {
                  ...errorContent(SavedObjectsErrorHelpers.createConflictError(type, id)),
                  // @ts-expect-error MultiGetHit._source is optional
                  ...(!this.rawDocExistsInNamespace(doc!, namespace) && {
                    metadata: { isNotOverwritable: true },
                  }),
                },
              });
            }
          })
          .catch((error: any) => {
            throw new Error(error);
          });
      })
    );

    return { errors };
  }

  /**
   * Deletes an object
   *
   * @param {string} type
   * @param {string} id
   * @param {object} [options={}]
   * @property {string} [options.namespace]
   * @returns {promise}
   */
  async delete(type: string, id: string, options: SavedObjectsDeleteOptions = {}): Promise<{}> {
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    const { refresh = DEFAULT_REFRESH_SETTING, force } = options;
    const namespace = normalizeNamespace(options.namespace);

    const rawId = this._serializer.generateRawId(namespace, type, id);
    let preflightResult: SavedObjectsRawDoc | undefined;

    if (this._registry.isMultiNamespace(type)) {
      preflightResult = await this.preflightCheckIncludesNamespace(type, id, namespace);
      const existingNamespaces = getSavedObjectNamespaces(undefined, preflightResult) ?? [];
      if (
        !force &&
        (existingNamespaces.length > 1 || existingNamespaces.includes(ALL_NAMESPACES_STRING))
      ) {
        throw SavedObjectsErrorHelpers.createBadRequestError(
          'Unable to delete saved object that exists in multiple namespaces, use the `force` option to delete it anyway'
        );
      }
    }

    const { body, statusCode } = await this.client.delete<DeleteDocumentResponse>(
      {
        id: rawId,
        index: this.getIndexForType(type),
        ...getExpectedVersionProperties(undefined, preflightResult),
        refresh,
      },
      { ignore: [404] }
    );

    const deleted = body.result === 'deleted';
    if (deleted) {
      return {};
    }

    const deleteDocNotFound = body.result === 'not_found';
    const deleteIndexNotFound = body.error && body.error.type === 'index_not_found_exception';
    if (deleteDocNotFound || deleteIndexNotFound) {
      // see "404s from missing index" above
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    throw new Error(
      `Unexpected OpenSearch DELETE response: ${JSON.stringify({
        type,
        id,
        response: { body, statusCode },
      })}`
    );
  }

  /**
   * Deletes all objects from the provided namespace.
   *
   * @param {string} namespace
   * @returns {promise} - { took, timed_out, total, deleted, batches, version_conflicts, noops, retries, failures }
   */
  async deleteByNamespace(
    namespace: string,
    options: SavedObjectsDeleteByNamespaceOptions = {}
  ): Promise<any> {
    if (!namespace || typeof namespace !== 'string' || namespace === '*') {
      throw new TypeError(`namespace is required, and must be a string that is not equal to '*'`);
    }

    const allTypes = Object.keys(getRootPropertiesObjects(this._mappings));
    const typesToUpdate = allTypes.filter((type) => !this._registry.isNamespaceAgnostic(type));

    const { body } = await this.client.updateByQuery(
      {
        index: this.getIndicesForTypes(typesToUpdate),
        refresh: options.refresh,
        body: {
          script: {
            source: `
              if (!ctx._source.containsKey('namespaces')) {
                ctx.op = "delete";
              } else {
                ctx._source['namespaces'].removeAll(Collections.singleton(params['namespace']));
                if (ctx._source['namespaces'].empty) {
                  ctx.op = "delete";
                }
              }
            `,
            lang: 'painless',
            params: { namespace },
          },
          conflicts: 'proceed',
          ...getSearchDsl(this._mappings, this._registry, {
            namespaces: namespace ? [namespace] : undefined,
            type: typesToUpdate,
          }),
        },
      },
      { ignore: [404] }
    );

    return body;
  }

  /**
   * Deletes all objects from the provided workspace.
   *
   * @param {string} workspace - workspace id
   * @param options SavedObjectsDeleteByWorkspaceOptions
   * @returns {promise} - { took, timed_out, total, deleted, batches, version_conflicts, noops, retries, failures }
   */
  async deleteByWorkspace(
    workspace: string,
    options: SavedObjectsDeleteByWorkspaceOptions = {}
  ): Promise<any> {
    if (!workspace || typeof workspace !== 'string' || workspace === '*') {
      throw new TypeError(`workspace is required, and must be a string that is not equal to '*'`);
    }

    const allTypes = Object.keys(getRootPropertiesObjects(this._mappings));

    const { body } = await this.client.updateByQuery(
      {
        index: this.getIndicesForTypes(allTypes),
        refresh: options.refresh,
        body: {
          script: {
            source: `
              if (!ctx._source.containsKey('workspaces')) {
                ctx.op = "delete";
              } else {
                ctx._source['workspaces'].removeAll(Collections.singleton(params['workspace']));
                if (ctx._source['workspaces'].empty) {
                  ctx.op = "delete";
                }
              }
            `,
            lang: 'painless',
            params: { workspace },
          },
          conflicts: 'proceed',
          ...getSearchDsl(this._mappings, this._registry, {
            workspaces: [workspace],
            type: allTypes,
          }),
        },
      },
      { ignore: [404] }
    );

    return body;
  }

  /**
   * @param {object} [options={}]
   * @property {(string|Array<string>)} [options.type]
   * @property {string} [options.search]
   * @property {string} [options.defaultSearchOperator]
   * @property {Array<string>} [options.searchFields] - see OpenSearch Simple Query String
   *                                        Query field argument for more information
   * @property {integer} [options.page=1]
   * @property {integer} [options.perPage=20]
   * @property {string} [options.sortField]
   * @property {string} [options.sortOrder]
   * @property {Array<string>} [options.fields]
   * @property {string} [options.namespace]
   * @property {object} [options.hasReference] - { type, id }
   * @property {string} [options.preference]
   * @returns {promise} - { saved_objects: [{ id, type, version, attributes }], total, per_page, page }
   */
  async find<T = unknown>(options: SavedObjectsFindOptions): Promise<SavedObjectsFindResponse<T>> {
    const {
      search,
      defaultSearchOperator = 'OR',
      searchFields,
      rootSearchFields,
      hasReference,
      page = FIND_DEFAULT_PAGE,
      perPage = FIND_DEFAULT_PER_PAGE,
      sortField,
      sortOrder,
      fields,
      namespaces,
      type,
      typeToNamespacesMap,
      filter,
      preference,
      workspaces,
      workspacesSearchOperator,
      ACLSearchParams,
    } = options;

    if (!type && !typeToNamespacesMap) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'options.type must be a string or an array of strings'
      );
    } else if (namespaces?.length === 0 && !typeToNamespacesMap) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'options.namespaces cannot be an empty array'
      );
    } else if (type && typeToNamespacesMap) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'options.type must be an empty string when options.typeToNamespacesMap is used'
      );
    } else if ((!namespaces || namespaces?.length) && typeToNamespacesMap) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'options.namespaces must be an empty array when options.typeToNamespacesMap is used'
      );
    }

    const types = type
      ? Array.isArray(type)
        ? type
        : [type]
      : Array.from(typeToNamespacesMap!.keys());
    const allowedTypes = types.filter((t) => this._allowedTypes.includes(t));
    if (allowedTypes.length === 0) {
      return SavedObjectsUtils.createEmptyFindResponse<T>(options);
    }

    if (searchFields && !Array.isArray(searchFields)) {
      throw SavedObjectsErrorHelpers.createBadRequestError('options.searchFields must be an array');
    }

    if (fields && !Array.isArray(fields)) {
      throw SavedObjectsErrorHelpers.createBadRequestError('options.fields must be an array');
    }

    let kueryNode;

    try {
      if (filter) {
        kueryNode = validateConvertFilterToKueryNode(allowedTypes, filter, this._mappings);
      }
    } catch (e) {
      if (e.name === 'DQLSyntaxError') {
        throw SavedObjectsErrorHelpers.createBadRequestError('DQLSyntaxError: ' + e.message);
      } else {
        throw e;
      }
    }

    const opensearchOptions = {
      index: this.getIndicesForTypes(allowedTypes),
      size: perPage,
      from: perPage * (page - 1),
      _source: includedFields(type, fields),
      rest_total_hits_as_int: true,
      preference,
      body: {
        seq_no_primary_term: true,
        ...getSearchDsl(this._mappings, this._registry, {
          search,
          defaultSearchOperator,
          searchFields,
          rootSearchFields,
          type: allowedTypes,
          sortField,
          sortOrder,
          namespaces,
          typeToNamespacesMap,
          hasReference,
          kueryNode,
          workspaces,
          workspacesSearchOperator,
          ACLSearchParams,
        }),
      },
    };

    const { body, statusCode } = await this.client.search<SavedObjectsRawDocSource>(
      opensearchOptions,
      {
        ignore: [404],
      }
    );
    if (statusCode === 404) {
      // 404 is only possible here if the index is missing, which
      // we don't want to leak, see "404s from missing index" above
      return {
        page,
        per_page: perPage,
        total: 0,
        saved_objects: [],
      };
    }

    return {
      page,
      per_page: perPage,
      total: body.hits.total,
      saved_objects: body.hits.hits.map(
        (hit: opensearchtypes.SearchHit<SavedObjectsRawDocSource>): SavedObjectsFindResult => ({
          // @ts-expect-error @opensearch-project/opensearch _source is optional
          ...this._rawToSavedObject(hit),
          score: hit._score!,
          // @ts-expect-error @opensearch-project/opensearch _source is optional
          sort: hit.sort,
        })
      ),
    } as SavedObjectsFindResponse<T>;
  }

  /**
   * Returns an array of objects by id
   *
   * @param {array} objects - an array of objects containing id, type and optionally fields
   * @param {object} [options={}]
   * @property {string} [options.namespace]
   * @returns {promise} - { saved_objects: [{ id, type, version, attributes }] }
   * @example
   *
   * bulkGet([
   *   { id: 'one', type: 'config' },
   *   { id: 'foo', type: 'index-pattern' }
   * ])
   */
  async bulkGet<T = unknown>(
    objects: SavedObjectsBulkGetObject[] = [],
    options: SavedObjectsBaseOptions = {}
  ): Promise<SavedObjectsBulkResponse<T>> {
    const namespace = normalizeNamespace(options.namespace);

    if (objects.length === 0) {
      return { saved_objects: [] };
    }

    let bulkGetRequestIndexCounter = 0;
    const expectedBulkGetResults: Either[] = objects.map((object) => {
      const { type, id, fields } = object;

      if (!this._allowedTypes.includes(type)) {
        return {
          tag: 'Left' as 'Left',
          error: {
            id,
            type,
            error: errorContent(SavedObjectsErrorHelpers.createUnsupportedTypeError(type)),
          },
        };
      }

      return {
        tag: 'Right' as 'Right',
        value: {
          type,
          id,
          fields,
          opensearchRequestIndex: bulkGetRequestIndexCounter++,
        },
      };
    });

    const bulkGetDocs = expectedBulkGetResults
      .filter(isRight)
      .map(({ value: { type, id, fields } }) => ({
        _id: this._serializer.generateRawId(namespace, type, id),
        _index: this.getIndexForType(type),
        _source: includedFields(type, fields),
      }));
    const bulkGetResponse = bulkGetDocs.length
      ? await this.client.mget(
          {
            body: {
              docs: bulkGetDocs,
            },
          },
          { ignore: [404] }
        )
      : undefined;

    return {
      saved_objects: expectedBulkGetResults.map((expectedResult) => {
        if (isLeft(expectedResult)) {
          return expectedResult.error as any;
        }

        const { type, id, opensearchRequestIndex } = expectedResult.value;
        const doc = bulkGetResponse?.body.docs[opensearchRequestIndex];

        if (!doc?.found || !this.rawDocExistsInNamespace(doc, namespace)) {
          return ({
            id,
            type,
            error: errorContent(SavedObjectsErrorHelpers.createGenericNotFoundError(type, id)),
          } as any) as SavedObject<T>;
        }

        return getSavedObjectFromSource(this._registry, type, id, doc);
      }),
    };
  }

  /**
   * Gets a single object
   *
   * @param {string} type
   * @param {string} id
   * @param {object} [options={}]
   * @property {string} [options.namespace]
   * @returns {promise} - { id, type, version, attributes }
   */
  async get<T = unknown>(
    type: string,
    id: string,
    options: SavedObjectsBaseOptions = {}
  ): Promise<SavedObject<T>> {
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    const namespace = normalizeNamespace(options.namespace);

    const { body, statusCode } = await this.client.get<SavedObjectsRawDocSource>(
      {
        id: this._serializer.generateRawId(namespace, type, id),
        index: this.getIndexForType(type),
      },
      { ignore: [404] }
    );

    const indexNotFound = statusCode === 404;
    if (
      !isFoundGetResponse(body) ||
      indexNotFound ||
      !this.rawDocExistsInNamespace(body, namespace)
    ) {
      // see "404s from missing index" above
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    const { originId, updated_at: updatedAt, permissions, workspaces } = body._source;

    let namespaces: string[] = [];
    if (!this._registry.isNamespaceAgnostic(type)) {
      namespaces = body._source.namespaces ?? [
        SavedObjectsUtils.namespaceIdToString(body._source.namespace),
      ];
    }

    return {
      id,
      type,
      namespaces,
      ...(originId && { originId }),
      ...(updatedAt && { updated_at: updatedAt }),
      ...(permissions && { permissions }),
      ...(workspaces && { workspaces }),
      version: encodeHitVersion(body),
      attributes: body._source[type],
      references: body._source.references || [],
      migrationVersion: body._source.migrationVersion,
    };
  }

  /**
   * Updates an object
   *
   * @param {string} type
   * @param {string} id
   * @param {object} [options={}]
   * @property {string} options.version - ensures version matches that of persisted object
   * @property {string} [options.namespace]
   * @property {array} [options.references] - [{ name, type, id }]
   * @returns {promise}
   */
  async update<T = unknown>(
    type: string,
    id: string,
    attributes: Partial<T>,
    options: SavedObjectsUpdateOptions = {}
  ): Promise<SavedObjectsUpdateResponse<T>> {
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    const { version, references, refresh = DEFAULT_REFRESH_SETTING, permissions } = options;
    const namespace = normalizeNamespace(options.namespace);

    let preflightResult: SavedObjectsRawDoc | undefined;
    if (this._registry.isMultiNamespace(type)) {
      preflightResult = await this.preflightCheckIncludesNamespace(type, id, namespace);
    }

    const time = this._getCurrentTime();

    const doc = {
      [type]: attributes,
      updated_at: time,
      ...(Array.isArray(references) && { references }),
      ...(permissions && { permissions }),
    };

    const { body, statusCode } = await this.client.update<SavedObjectsRawDocSource>(
      {
        id: this._serializer.generateRawId(namespace, type, id),
        index: this.getIndexForType(type),
        ...getExpectedVersionProperties(version, preflightResult),
        refresh,

        body: {
          doc,
        },
        _source_includes: ['namespace', 'namespaces', 'originId'],
      },
      { ignore: [404] }
    );

    if (statusCode === 404) {
      // see "404s from missing index" above
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    const { originId } = body.get?._source ?? {};
    let namespaces: string[] = [];
    if (!this._registry.isNamespaceAgnostic(type)) {
      namespaces = body.get?._source.namespaces ?? [
        SavedObjectsUtils.namespaceIdToString(body.get?._source.namespace),
      ];
    }

    return {
      id,
      type,
      updated_at: time,
      version: encodeHitVersion(body),
      namespaces,
      ...(originId && { originId }),
      ...(permissions && { permissions }),
      references,
      attributes,
    };
  }

  /**
   * Adds one or more namespaces to a given multi-namespace saved object. This method and
   * [`deleteFromNamespaces`]{@link SavedObjectsRepository.deleteFromNamespaces} are the only ways to change which Spaces a multi-namespace
   * saved object is shared to.
   */
  async addToNamespaces(
    type: string,
    id: string,
    namespaces: string[],
    options: SavedObjectsAddToNamespacesOptions = {}
  ): Promise<SavedObjectsAddToNamespacesResponse> {
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    if (!this._registry.isMultiNamespace(type)) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        `${type} doesn't support multiple namespaces`
      );
    }

    if (!namespaces.length) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'namespaces must be a non-empty array of strings'
      );
    }

    const { version, namespace, refresh = DEFAULT_REFRESH_SETTING } = options;
    // we do not need to normalize the namespace to its ID format, since it will be converted to a namespace string before being used

    const rawId = this._serializer.generateRawId(undefined, type, id);
    const preflightResult = await this.preflightCheckIncludesNamespace(type, id, namespace);
    const existingNamespaces = getSavedObjectNamespaces(undefined, preflightResult);
    // there should never be a case where a multi-namespace object does not have any existing namespaces
    // however, it is a possibility if someone manually modifies the document in OpenSearch
    const time = this._getCurrentTime();

    const doc = {
      updated_at: time,
      namespaces: existingNamespaces ? unique(existingNamespaces.concat(namespaces)) : namespaces,
    };

    const { statusCode } = await this.client.update(
      {
        id: rawId,
        index: this.getIndexForType(type),
        ...getExpectedVersionProperties(version, preflightResult),
        refresh,
        body: {
          doc,
        },
      },
      { ignore: [404] }
    );

    if (statusCode === 404) {
      // see "404s from missing index" above
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    return { namespaces: doc.namespaces };
  }

  /**
   * Removes one or more namespaces from a given multi-namespace saved object. If no namespaces remain, the saved object is deleted
   * entirely. This method and [`addToNamespaces`]{@link SavedObjectsRepository.addToNamespaces} are the only ways to change which Spaces a
   * multi-namespace saved object is shared to.
   */
  async deleteFromNamespaces(
    type: string,
    id: string,
    namespaces: string[],
    options: SavedObjectsDeleteFromNamespacesOptions = {}
  ): Promise<SavedObjectsDeleteFromNamespacesResponse> {
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }

    if (!this._registry.isMultiNamespace(type)) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        `${type} doesn't support multiple namespaces`
      );
    }

    if (!namespaces.length) {
      throw SavedObjectsErrorHelpers.createBadRequestError(
        'namespaces must be a non-empty array of strings'
      );
    }

    const { namespace, refresh = DEFAULT_REFRESH_SETTING } = options;
    // we do not need to normalize the namespace to its ID format, since it will be converted to a namespace string before being used

    const rawId = this._serializer.generateRawId(undefined, type, id);
    const preflightResult = await this.preflightCheckIncludesNamespace(type, id, namespace);
    const existingNamespaces = getSavedObjectNamespaces(undefined, preflightResult);
    // if there are somehow no existing namespaces, allow the operation to proceed and delete this saved object
    const remainingNamespaces = existingNamespaces?.filter((x) => !namespaces.includes(x));

    if (remainingNamespaces?.length) {
      // if there is 1 or more namespace remaining, update the saved object
      const time = this._getCurrentTime();

      const doc = {
        updated_at: time,
        namespaces: remainingNamespaces,
      };

      const { statusCode } = await this.client.update(
        {
          id: rawId,
          index: this.getIndexForType(type),
          ...getExpectedVersionProperties(undefined, preflightResult),
          refresh,

          body: {
            doc,
          },
        },
        {
          ignore: [404],
        }
      );

      if (statusCode === 404) {
        // see "404s from missing index" above
        throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
      }
      return { namespaces: doc.namespaces };
    } else {
      // if there are no namespaces remaining, delete the saved object
      const { body, statusCode } = await this.client.delete<DeleteDocumentResponse>(
        {
          id: this._serializer.generateRawId(undefined, type, id),
          refresh,
          ...getExpectedVersionProperties(undefined, preflightResult),
          index: this.getIndexForType(type),
        },
        {
          ignore: [404],
        }
      );

      const deleted = body.result === 'deleted';
      if (deleted) {
        return { namespaces: [] };
      }

      const deleteDocNotFound = body.result === 'not_found';
      const deleteIndexNotFound = body.error && body.error.type === 'index_not_found_exception';
      if (deleteDocNotFound || deleteIndexNotFound) {
        // see "404s from missing index" above
        throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
      }

      throw new Error(
        `Unexpected OpenSearch DELETE response: ${JSON.stringify({
          type,
          id,
          response: { body, statusCode },
        })}`
      );
    }
  }

  /**
   * Updates multiple objects in bulk
   *
   * @param {array} objects - [{ type, id, attributes, options: { version, namespace } references }]
   * @property {string} options.version - ensures version matches that of persisted object
   * @property {string} [options.namespace]
   * @returns {promise} -  {saved_objects: [[{ id, type, version, references, attributes, error: { message } }]}
   */
  async bulkUpdate<T = unknown>(
    objects: Array<SavedObjectsBulkUpdateObject<T>>,
    options: SavedObjectsBulkUpdateOptions = {}
  ): Promise<SavedObjectsBulkUpdateResponse<T>> {
    const time = this._getCurrentTime();
    const namespace = normalizeNamespace(options.namespace);

    let bulkGetRequestIndexCounter = 0;
    const expectedBulkGetResults: Either[] = objects.map((object) => {
      const { type, id } = object;

      if (!this._allowedTypes.includes(type)) {
        return {
          tag: 'Left' as 'Left',
          error: {
            id,
            type,
            error: errorContent(SavedObjectsErrorHelpers.createGenericNotFoundError(type, id)),
          },
        };
      }

      const { attributes, references, version, namespace: objectNamespace, permissions } = object;

      if (objectNamespace === ALL_NAMESPACES_STRING) {
        return {
          tag: 'Left' as 'Left',
          error: {
            id,
            type,
            error: errorContent(
              SavedObjectsErrorHelpers.createBadRequestError('"namespace" cannot be "*"')
            ),
          },
        };
      }
      // `objectNamespace` is a namespace string, while `namespace` is a namespace ID.
      // The object namespace string, if defined, will supersede the operation's namespace ID.

      const documentToSave = {
        [type]: attributes,
        updated_at: time,
        ...(Array.isArray(references) && { references }),
        ...(permissions && { permissions }),
      };

      const requiresNamespacesCheck = this._registry.isMultiNamespace(object.type);

      return {
        tag: 'Right' as 'Right',
        value: {
          type,
          id,
          version,
          documentToSave,
          objectNamespace,
          ...(requiresNamespacesCheck && { opensearchRequestIndex: bulkGetRequestIndexCounter++ }),
        },
      };
    });

    const getNamespaceId = (objectNamespace?: string) =>
      objectNamespace !== undefined
        ? SavedObjectsUtils.namespaceStringToId(objectNamespace)
        : namespace;
    const getNamespaceString = (objectNamespace?: string) =>
      objectNamespace ?? SavedObjectsUtils.namespaceIdToString(namespace);

    const bulkGetDocs = expectedBulkGetResults
      .filter(isRight)
      .filter(({ value }) => value.opensearchRequestIndex !== undefined)
      .map(({ value: { type, id, objectNamespace } }) => ({
        _id: this._serializer.generateRawId(getNamespaceId(objectNamespace), type, id),
        _index: this.getIndexForType(type),
        _source: ['type', 'namespaces'],
      }));
    const bulkGetResponse = bulkGetDocs.length
      ? await this.client.mget(
          {
            body: {
              docs: bulkGetDocs,
            },
          },
          {
            ignore: [404],
          }
        )
      : undefined;

    let bulkUpdateRequestIndexCounter = 0;
    const bulkUpdateParams: object[] = [];
    const expectedBulkUpdateResults: Either[] = expectedBulkGetResults.map(
      (expectedBulkGetResult) => {
        if (isLeft(expectedBulkGetResult)) {
          return expectedBulkGetResult;
        }

        const {
          opensearchRequestIndex,
          id,
          type,
          version,
          documentToSave,
          objectNamespace,
        } = expectedBulkGetResult.value;

        let namespaces;
        let versionProperties;
        if (opensearchRequestIndex !== undefined) {
          const indexFound = bulkGetResponse?.statusCode !== 404;
          const actualResult = indexFound
            ? bulkGetResponse?.body.docs[opensearchRequestIndex]
            : undefined;
          const docFound = indexFound && actualResult?.found === true;
          if (
            !docFound ||
            !this.rawDocExistsInNamespace(actualResult, getNamespaceId(objectNamespace))
          ) {
            return {
              tag: 'Left' as 'Left',
              error: {
                id,
                type,
                error: errorContent(SavedObjectsErrorHelpers.createGenericNotFoundError(type, id)),
              },
            };
          }
          namespaces = actualResult!._source.namespaces ?? [
            SavedObjectsUtils.namespaceIdToString(actualResult!._source.namespace),
          ];

          versionProperties = getExpectedVersionProperties(version, actualResult);
        } else {
          if (this._registry.isSingleNamespace(type)) {
            // if `objectNamespace` is undefined, fall back to `options.namespace`
            namespaces = [getNamespaceString(objectNamespace)];
          }
          versionProperties = getExpectedVersionProperties(version);
        }

        const expectedResult = {
          type,
          id,
          namespaces,
          opensearchRequestIndex: bulkUpdateRequestIndexCounter++,
          documentToSave: expectedBulkGetResult.value.documentToSave,
        };

        bulkUpdateParams.push(
          {
            update: {
              _id: this._serializer.generateRawId(getNamespaceId(objectNamespace), type, id),
              _index: this.getIndexForType(type),
              ...versionProperties,
            },
          },
          { doc: documentToSave }
        );

        return { tag: 'Right' as 'Right', value: expectedResult };
      }
    );

    const { refresh = DEFAULT_REFRESH_SETTING } = options;
    const bulkUpdateResponse = bulkUpdateParams.length
      ? await this.client.bulk({
          refresh,
          body: bulkUpdateParams,
          _source_includes: ['originId'],
        })
      : undefined;

    return {
      saved_objects: expectedBulkUpdateResults.map((expectedResult) => {
        if (isLeft(expectedResult)) {
          return expectedResult.error as any;
        }

        const {
          type,
          id,
          namespaces,
          documentToSave,
          opensearchRequestIndex,
        } = expectedResult.value;
        const response = bulkUpdateResponse?.body.items[opensearchRequestIndex] ?? {};
        // When a bulk update operation is completed, any fields specified in `_sourceIncludes` will be found in the "get" value of the
        // returned object. We need to retrieve the `originId` if it exists so we can return it to the consumer.
        const { error, _seq_no: seqNo, _primary_term: primaryTerm, get } = Object.values(
          response
        )[0] as any;

        // eslint-disable-next-line @typescript-eslint/naming-convention
        const { [type]: attributes, references, updated_at, permissions } = documentToSave;
        if (error) {
          return {
            id,
            type,
            error: getBulkOperationError(error, type, id),
          };
        }

        const { originId } = get._source;
        return {
          id,
          type,
          ...(namespaces && { namespaces }),
          ...(originId && { originId }),
          updated_at,
          version: encodeVersion(seqNo, primaryTerm),
          attributes,
          references,
          ...(permissions && { permissions }),
        };
      }),
    };
  }

  /**
   * Increases a counter field by incrementValue which by default is 1. Creates the document if one doesn't exist for the given id.
   *
   * @param {string} type
   * @param {string} id
   * @param {string} counterFieldName
   * @param {object} [options={}]
   * @param {number} [incrementValue=1]
   * @property {object} [options.migrationVersion=undefined]
   * @returns {promise}
   */
  async incrementCounter(
    type: string,
    id: string,
    counterFieldName: string,
    options: ExternalSavedObjectsIncrementCounterOptions = {},
    incrementValue: number = 1
  ): Promise<SavedObject> {
    if (typeof type !== 'string') {
      throw new Error('"type" argument must be a string');
    }
    if (typeof counterFieldName !== 'string') {
      throw new Error('"counterFieldName" argument must be a string');
    }
    if (!this._allowedTypes.includes(type)) {
      throw SavedObjectsErrorHelpers.createUnsupportedTypeError(type);
    }

    const { migrationVersion, refresh = DEFAULT_REFRESH_SETTING } = options;
    const namespace = normalizeNamespace(options.namespace);

    const time = this._getCurrentTime();
    let savedObjectNamespace;
    let savedObjectNamespaces: string[] | undefined;

    if (this._registry.isSingleNamespace(type) && namespace) {
      savedObjectNamespace = namespace;
    } else if (this._registry.isMultiNamespace(type)) {
      savedObjectNamespaces = await this.preflightGetNamespaces(type, id, namespace);
    }

    const raw = this._serializer.savedObjectToRaw({
      id,
      type,
      ...(savedObjectNamespace && { namespace: savedObjectNamespace }),
      ...(savedObjectNamespaces && { namespaces: savedObjectNamespaces }),
      attributes: { [counterFieldName]: incrementValue },
      migrationVersion,
      updated_at: time,
    } as SavedObjectSanitizedDoc);
    const { body } = await this.client.update<SavedObjectsRawDocSource>({
      id: raw._id,
      index: this.getIndexForType(type),
      refresh,
      _source: 'true',
      body: {
        script: {
          source: `
              if (ctx._source[params.type][params.counterFieldName] == null) {
                ctx._source[params.type][params.counterFieldName] = params.count;
              }
              else {
                ctx._source[params.type][params.counterFieldName] += params.count;
              }
              ctx._source.updated_at = params.time;
            `,
          lang: 'painless',
          params: {
            count: incrementValue,
            time,
            type,
            counterFieldName,
          },
        },
        upsert: raw._source,
      },
    });
    const { originId } = body.get?._source ?? {};
    return {
      id,
      type,
      ...(savedObjectNamespaces && { namespaces: savedObjectNamespaces }),
      ...(originId && { originId }),
      updated_at: time,
      references: body.get?._source.references ?? [],
      version: encodeHitVersion(body),
      attributes: body.get?._source[type],
    };
  }

  /**
   * Returns index specified by the given type or the default index
   *
   * @param type - the type
   */
  private getIndexForType(type: string) {
    return this._registry.getIndex(type) || this._index;
  }

  /**
   * Returns an array of indices as specified in `this._registry` for each of the
   * given `types`. If any of the types don't have an associated index, the
   * default index `this._index` will be included.
   *
   * @param types The types whose indices should be retrieved
   */
  private getIndicesForTypes(types: string[]) {
    return unique(types.map((t) => this.getIndexForType(t)));
  }

  private _getCurrentTime() {
    return new Date().toISOString();
  }

  private _rawToSavedObject<T = unknown>(raw: SavedObjectsRawDoc): SavedObject<T> {
    const savedObject = this._serializer.rawToSavedObject(raw);
    const { namespace, type } = savedObject;
    if (this._registry.isSingleNamespace(type)) {
      savedObject.namespaces = [SavedObjectsUtils.namespaceIdToString(namespace)];
    }
    return omit(savedObject, 'namespace') as SavedObject<T>;
  }

  /**
   * Check to ensure that a raw document exists in a namespace. If the document is not a multi-namespace type, then this returns `true` as
   * we rely on the guarantees of the document ID format. If the document is a multi-namespace type, this checks to ensure that the
   * document's `namespaces` value includes the string representation of the given namespace.
   *
   * WARNING: This should only be used for documents that were retrieved from OpenSearch. Otherwise, the guarantees of the document ID
   * format mentioned above do not apply.
   */
  private rawDocExistsInNamespace(raw: SavedObjectsRawDoc, namespace: string | undefined) {
    const rawDocType = raw._source.type;

    // if the type is namespace isolated, or namespace agnostic, we can continue to rely on the guarantees
    // of the document ID format and don't need to check this
    if (!this._registry.isMultiNamespace(rawDocType)) {
      return true;
    }

    const namespaces = raw._source.namespaces;
    const existsInNamespace =
      namespaces?.includes(SavedObjectsUtils.namespaceIdToString(namespace)) ||
      namespaces?.includes('*');
    return existsInNamespace ?? false;
  }

  /**
   * Pre-flight check to get a multi-namespace saved object's included namespaces. This ensures that, if the saved object exists, it
   * includes the target namespace.
   *
   * @param type The type of the saved object.
   * @param id The ID of the saved object.
   * @param namespace The target namespace.
   * @returns Array of namespaces that this saved object currently includes, or (if the object does not exist yet) the namespaces that a
   * newly-created object will include. Value may be undefined if an existing saved object has no namespaces attribute; this should not
   * happen in normal operations, but it is possible if the OpenSearch document is manually modified.
   * @throws Will throw an error if the saved object exists and it does not include the target namespace.
   */
  private async preflightGetNamespaces(type: string, id: string, namespace?: string) {
    if (!this._registry.isMultiNamespace(type)) {
      throw new Error(`Cannot make preflight get request for non-multi-namespace type '${type}'.`);
    }

    const { body, statusCode } = await this.client.get<SavedObjectsRawDocSource>(
      {
        id: this._serializer.generateRawId(undefined, type, id),
        index: this.getIndexForType(type),
      },
      {
        ignore: [404],
      }
    );

    const indexFound = statusCode !== 404;
    if (indexFound && isFoundGetResponse(body)) {
      if (!this.rawDocExistsInNamespace(body, namespace)) {
        throw SavedObjectsErrorHelpers.createConflictError(type, id);
      }
      return getSavedObjectNamespaces(namespace, body);
    }
    return getSavedObjectNamespaces(namespace);
  }

  /**
   * Pre-flight check for a multi-namespace saved object's namespaces. This ensures that, if the saved object exists, it includes the target
   * namespace.
   *
   * @param type The type of the saved object.
   * @param id The ID of the saved object.
   * @param namespace The target namespace.
   * @returns Raw document from OpenSearch.
   * @throws Will throw an error if the saved object is not found, or if it doesn't include the target namespace.
   */
  private async preflightCheckIncludesNamespace(type: string, id: string, namespace?: string) {
    if (!this._registry.isMultiNamespace(type)) {
      throw new Error(`Cannot make preflight get request for non-multi-namespace type '${type}'.`);
    }

    const rawId = this._serializer.generateRawId(undefined, type, id);
    const { body, statusCode } = await this.client.get<SavedObjectsRawDocSource>(
      {
        id: rawId,
        index: this.getIndexForType(type),
      },
      { ignore: [404] }
    );

    const indexFound = statusCode !== 404;
    if (
      !indexFound ||
      !isFoundGetResponse(body) ||
      !this.rawDocExistsInNamespace(body, namespace)
    ) {
      throw SavedObjectsErrorHelpers.createGenericNotFoundError(type, id);
    }
    return body;
  }
}
