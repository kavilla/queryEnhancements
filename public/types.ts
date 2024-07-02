/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { DataPublicPluginSetup, DataPublicPluginStart } from 'src/plugins/data/public';
import { UiActionsStart } from 'src/plugins/ui_actions/public';

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginSetup {}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface QueryEnhancementsPluginStart {}

export interface QueryEnhancementsPluginSetupDependencies {
  data: DataPublicPluginSetup;
  uiActions: UiActionsStart;
}

export interface QueryEnhancementsPluginStartDependencies {
  data: DataPublicPluginStart;
}
