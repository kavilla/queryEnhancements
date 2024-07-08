/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { CoreSetup } from 'opensearch-dashboards/public';
import { Observable } from 'rxjs';
import { ASYNC_TRIGGER_ID } from './constants';

export interface FetchDataFrameContext {
  http: CoreSetup['http'];
  path: string;
  signal?: AbortSignal;
}

export type FetchFunction<T, P = void> = (params?: P) => Observable<T>;

/**
 * Job states are defined for compatibility with EMR job run states, but aren't strictly limited to
 * that use. See also: {@link parseJobState}.
 */
export enum JobState {
  SUBMITTED = 'SUBMITTED',
  PENDING = 'PENDING',
  SCHEDULED = 'SCHEDULING',
  RUNNING = 'RUNNING',
  FAILED = 'FAILED',
  SUCCESS = 'SUCCESS',
  CANCELLING = 'CANCELLING',
  CANCELLED = 'CANCELLED',
}

/**
 * Convert a string to a {@link JobState} if possible. Case-insensitive.
 *
 * @param maybeState An optional string.
 * @returns The corresponding {@link JobState} if one exists, otherwise undefined.
 */
export const parseJobState = (maybeState: string | undefined): JobState | undefined => {
  if (maybeState === undefined) {
    // Simplifies using the method with possibly-optional values.
    return undefined;
  }
  maybeState = maybeState.toUpperCase();
  return Object.values(JobState).find((state) => state === maybeState);
};

export interface AsyncQueryContext {
  query_id: string;
  query_status: JobState;
}
declare module '../../../src/plugins/ui_actions/public' {
  export interface TriggerContextMapping {
    [ASYNC_TRIGGER_ID]: AsyncQueryContext;
  }
}
