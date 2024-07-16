/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { SharedGlobalConfig, Logger, ILegacyClusterClient } from 'opensearch-dashboards/server';
import { Observable } from 'rxjs';
import { ISearchStrategy, SearchUsage } from '../../../../src/plugins/data/server';
import {
  DATA_FRAME_TYPES,
  IDataFrameError,
  IDataFrameResponse,
  IOpenSearchDashboardsSearchRequest,
  PartialDataFrame,
  createDataFrame,
} from '../../../../src/plugins/data/common';
import { Facet } from '../utils';

export const pplAsyncSearchStrategyProvider = (
  config$: Observable<SharedGlobalConfig>,
  logger: Logger,
  client: ILegacyClusterClient,
  usage?: SearchUsage
): ISearchStrategy<IOpenSearchDashboardsSearchRequest, IDataFrameResponse> => {
  const pplAsyncFacet = new Facet({ client, logger, endpoint: 'observability.runDirectQuery' });
  const pplAsyncJobsFacet = new Facet({
    client,
    logger,
    endpoint: 'observability.getJobStatus',
    useJobs: true,
  });

  return {
    search: async (context, request: any, options) => {
      try {
        // Create job: this should return a queryId and sessionId
        if (request?.body?.query?.qs) {
          const df = request.body?.df;
          request.body = {
            query: request.body.query.qs,
            datasource: df?.meta?.queryConfig?.dataSource,
            lang: 'ppl',
            sessionId: df?.meta?.sessionId,
          };
          const rawResponse: any = await pplAsyncFacet.describeQuery(context, request);
          // handles failure
          if (!rawResponse.success) {
            return {
              type: DATA_FRAME_TYPES.POLLING,
              body: { error: rawResponse.data },
              took: rawResponse.took,
            } as IDataFrameError;
          }
          const queryId = rawResponse.data?.queryId;
          const sessionId = rawResponse.data?.sessionId;

          const partial: PartialDataFrame = {
            name: '',
            fields: rawResponse?.data?.schema || [],
          };
          const dataFrame = createDataFrame(partial);
          dataFrame.meta = {
            query: request.body.query,
            queryId,
            sessionId,
          };
          dataFrame.name = request.body?.datasource;
          return {
            type: DATA_FRAME_TYPES.POLLING,
            body: dataFrame,
            took: rawResponse.took,
          } as IDataFrameResponse;
        } else {
          const queryId = request.params.queryId;
          request.params = { queryId };
          const asyncResponse: any = await pplAsyncJobsFacet.describeQuery(context, request);
          const status = asyncResponse.data.status;
          const partial: PartialDataFrame = {
            name: '',
            fields: asyncResponse?.data?.schema || [],
          };
          const dataFrame = createDataFrame(partial);
          dataFrame.fields.forEach((field, index) => {
            field.values = asyncResponse?.data.datarows.map((row: any) => row[index]);
          });

          dataFrame.size = asyncResponse?.data?.datarows?.length || 0;

          dataFrame.meta = {
            status,
            queryId,
            error: status === 'FAILED' && asyncResponse.data?.error,
          };
          dataFrame.name = request.body?.datasource;

          // TODO: MQL should this be the time for polling or the time for job creation?
          if (usage) usage.trackSuccess(asyncResponse.took);

          return {
            type: DATA_FRAME_TYPES.POLLING,
            body: dataFrame,
            took: asyncResponse.took,
          } as IDataFrameResponse;
        }
      } catch (e) {
        logger.error(`pplAsyncSearchStrategy: ${e.message}`);
        if (usage) usage.trackError();
        throw e;
      }
    },
  };
};
