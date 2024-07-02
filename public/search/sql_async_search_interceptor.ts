import { trimEnd } from 'lodash';
import { BehaviorSubject, Observable, throwError } from 'rxjs';
import { i18n } from '@osd/i18n';
import { concatMap, map } from 'rxjs/operators';
import {
  DATA_FRAME_TYPES,
  DataPublicPluginStart,
  IOpenSearchDashboardsSearchRequest,
  IOpenSearchDashboardsSearchResponse,
  ISearchOptions,
  SearchInterceptor,
  SearchInterceptorDeps,
} from '../../../../src/plugins/data/public';
import {
  getRawDataFrame,
  getRawQueryString,
  IDataFrameResponse,
} from '../../../../src/plugins/data/common';
import {
  API,
  ASYNC_TRIGGER_ID,
  DataFramePolling,
  FetchDataFrameContext,
  SEARCH_STRATEGY,
  SparkJobState,
  fetchDataFrame,
  fetchDataFramePolling,
} from '../../common';
import { QueryEnhancementsPluginStartDependencies } from '../types';
import { UiActionsStart } from 'src/plugins/ui_actions/public';

export class SQLAsyncSearchInterceptor extends SearchInterceptor {
  protected queryService!: DataPublicPluginStart['query'];
  protected aggsService!: DataPublicPluginStart['search']['aggs'];
  protected dataFrame$ = new BehaviorSubject<IDataFrameResponse | undefined>(undefined);
  protected uiActions: UiActionsStart;

  constructor(deps: SearchInterceptorDeps) {
    super(deps);
    this.uiActions = deps.uiActions;

    deps.startServices.then(([_coreStart, depsStart]) => {
      this.queryService = (depsStart as QueryEnhancementsPluginStartDependencies).data.query;
      this.aggsService = (depsStart as QueryEnhancementsPluginStartDependencies).data.search.aggs;
    });
  }

  protected runSearch(
    request: IOpenSearchDashboardsSearchRequest,
    signal?: AbortSignal,
    _strategy?: string
  ): Observable<IOpenSearchDashboardsSearchResponse> {
    const { id, ...searchRequest } = request;
    const path = trimEnd(API.SQL_ASYNC_SEARCH);
    const dfContext: FetchDataFrameContext = {
      http: this.deps.http,
      path,
      signal,
    };

    const dataFrame = getRawDataFrame(searchRequest);
    if (!dataFrame) {
      return throwError(this.handleSearchError('DataFrame is not defined', request, signal!));
    }

    const queryString =
      dataFrame.meta?.queryConfig?.formattedQs() ?? getRawQueryString(searchRequest) ?? '';

    const onPollingSuccess = (pollingResult: any) => {
      if (pollingResult) {
        const statusStr: string = (pollingResult.body.meta.status as string).toUpperCase();
        const status = SparkJobState[statusStr as keyof typeof SparkJobState];
        switch (status) {
          case SparkJobState.SUCCESS:
            return false;
          case SparkJobState.FAILED:
            const jsError = new Error(pollingResult.data.error.response);
            this.deps.toasts.addError(jsError, {
              title: i18n.translate('queryEnhancements.sqlQueryError', {
                defaultMessage: 'Could not complete the SQL async query',
              }),
              toastMessage: pollingResult.data.error.response,
            });
            return false;
          default:
            if (request.params?.progress_query_id) {
              this.uiActions.getTrigger(ASYNC_TRIGGER_ID).exec({
                query_id: request.params.progress_query_id,
                query_status: status,
              });
            }
        }
      }

      this.deps.toasts.addInfo({
        title: i18n.translate('queryEnhancements.sqlQueryPolling', {
          defaultMessage: 'Polling query job results...',
        }),
      });

      return true;
    };

    const onPollingError = (error: Error) => {
      throw new Error(error.message);
    };

    this.deps.toasts.addInfo({
      title: i18n.translate('queryEnhancements.sqlQueryInfo', {
        defaultMessage: 'Starting query job...',
      }),
    });
    return fetchDataFrame(dfContext, queryString, dataFrame).pipe(
      concatMap((jobResponse) => {
        const df = jobResponse.body;
        const dataFramePolling = new DataFramePolling<any, any>(
          () => fetchDataFramePolling(dfContext, df),
          5000,
          onPollingSuccess,
          onPollingError
        );
        return dataFramePolling.fetch().pipe(
          map(() => {
            const dfPolling = dataFramePolling.data;
            dfPolling.type = DATA_FRAME_TYPES.DEFAULT;
            return dfPolling;
          })
        );
      })
    );
  }

  public search(request: IOpenSearchDashboardsSearchRequest, options: ISearchOptions) {
    return this.runSearch(request, options.abortSignal, SEARCH_STRATEGY.SQL_ASYNC);
  }
}
