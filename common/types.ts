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

// ref: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/job-states.html
export enum SparkJobState {
  SUBMITTED,
  PENDING,
  SCHEDULED,
  RUNNING,
  FAILED,
  SUCCESS,
  CANCELLING,
  CANCELLED,
}

export interface AsyncQueryContext {
  query_id: string;
  query_status: SparkJobState;
}
declare module '../../../src/plugins/ui_actions/public' {
  export interface TriggerContextMapping {
    [ASYNC_TRIGGER_ID]: AsyncQueryContext;
  }
}
