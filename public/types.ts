/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { CoreSetup, CoreStart } from 'opensearch-dashboards/public';
import { DataPublicPluginSetup, DataPublicPluginStart } from 'src/plugins/data/public';
import { DataSourcePluginStart } from 'src/plugins/data_source/public';

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginSetup {}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginStart {}

export interface QueryEnhancementsPluginSetupDependencies {
  data: DataPublicPluginSetup;
}

export interface QueryEnhancementsPluginStartDependencies {
  data: DataPublicPluginStart;
  dataSource?: DataSourcePluginStart;
}

export interface Connection {
  id: string;
  title: string;
  endpoint?: string;
  installedPlugins?: string[];
  auth?: any;
}

export interface ConnectionsServiceDeps {
  http: CoreSetup['http'];
  startServices: Promise<[CoreStart, QueryEnhancementsPluginStartDependencies, unknown]>;
}
