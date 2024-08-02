/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { DataPluginSetup } from 'src/plugins/data/server/plugin';
import { IndexMapping } from 'src/core/server/saved_objects/mappings';
import { opensearchtypes } from '@opensearch-project/opensearch/.';
import { DataSourcePluginSetup } from '../../../src/plugins/data_source/server';
import {
  ILegacyClusterClient,
  Logger,
  MutatingOperationRefreshSetting,
  SavedObjectTypeRegistry,
  SavedObjectsBaseOptions,
  SavedObjectsMigrationVersion,
  SavedObjectsSerializer,
} from '../../../src/core/server';
import { ConfigSchema } from '../common/config';
import { ExternalSavedObjectsRepository } from './saved_objects/external_repository';

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginSetup {}
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginStart {}

export interface QueryEnhancementsPluginSetupDependencies {
  data: DataPluginSetup;
  dataSource?: DataSourcePluginSetup;
}

export interface ISchema {
  name: string;
  type: string;
}

export interface IPPLDataSource {
  jsonData?: any[];
}

export interface IPPLVisualizationDataSource extends IPPLDataSource {
  data: any;
  metadata: any;
  size: number;
  status: number;
}

export interface IPPLEventsDataSource extends IPPLDataSource {
  schema: ISchema[];
  datarows: any[];
}

export interface FacetResponse {
  success: boolean;
  data: any;
}

export interface FacetRequest {
  body: {
    query: string;
    format?: string;
  };
}

export interface ExternalSavedObjectsRepositoryOptions {
  index: string;
  mappings: IndexMapping;
  client: ILegacyClusterClient;
  typeRegistry: SavedObjectTypeRegistry;
  serializer: SavedObjectsSerializer;
  allowedTypes: string[];
}

export interface ExternalSavedObjectsIncrementCounterOptions extends SavedObjectsBaseOptions {
  migrationVersion?: SavedObjectsMigrationVersion;
  /** The OpenSearch Refresh setting for this operation */
  refresh?: MutatingOperationRefreshSetting;
}

export interface ExternalSavedObjectsDeleteByNamespaceOptions extends SavedObjectsBaseOptions {
  /** The OpenSearch supports only boolean flag for this operation */
  refresh?: boolean;
}

export type IExternalSavedObjectsRepository = Pick<
  ExternalSavedObjectsRepository,
  keyof ExternalSavedObjectsRepository
>;

export type GetResponseFound<TDocument = unknown> = opensearchtypes.GetResponse<TDocument> &
  Required<
    Pick<
      opensearchtypes.GetResponse<TDocument>,
      '_primary_term' | '_seq_no' | '_version' | '_source'
    >
  >;

declare module '../../../src/core/server' {
  interface RequestHandlerContext {
    query_assist: {
      logger: Logger;
      configPromise: Promise<ConfigSchema>;
      dataSourceEnabled: boolean;
    };
    data_source_connection: {
      logger: Logger;
      configPromise: Promise<ConfigSchema>;
      dataSourceEnabled: boolean;
    };
  }
}
