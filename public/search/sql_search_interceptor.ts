/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { trimEnd } from 'lodash';
import { Observable, from } from 'rxjs';
import { stringify } from '@osd/std';
import { i18n } from '@osd/i18n';
import {
  DataPublicPluginStart,
  IOpenSearchDashboardsSearchRequest,
  IOpenSearchDashboardsSearchResponse,
  ISearchOptions,
  SearchInterceptor,
  SearchInterceptorDeps,
} from '../../../../src/plugins/data/public';
import { getRawDataSource, removeRawDataSourceFromQs } from '../../../../src/plugins/data/common';
import { API, SEARCH_STRATEGY } from '../../common';
import { QueryEnhancementsPluginStartDependencies } from '../types';

export class SQLSearchInterceptor extends SearchInterceptor {
  protected queryService!: DataPublicPluginStart['query'];
  protected aggsService!: DataPublicPluginStart['search']['aggs'];

  constructor(deps: SearchInterceptorDeps) {
    super(deps);

    deps.startServices.then(([coreStart, depsStart]) => {
      this.queryService = (depsStart as QueryEnhancementsPluginStartDependencies).data.query;
      this.aggsService = (depsStart as QueryEnhancementsPluginStartDependencies).data.search.aggs;
    });
  }

  protected runSearch(
    request: IOpenSearchDashboardsSearchRequest,
    signal?: AbortSignal,
    strategy?: string
  ): Observable<IOpenSearchDashboardsSearchResponse> {
    const { id, ...searchRequest } = request;
    const path = trimEnd(API.SQL_SEARCH);

    const fetchDataFrame = (queryString: string, df = null) => {
      const rawDataSource = getRawDataSource(queryString);
      const body = stringify({
        query: { qs: removeRawDataSourceFromQs(queryString), format: 'jdbc' },
        df,
        ...(rawDataSource ? { dataSourceId: rawDataSource } : {}),
      });
      return from(
        this.deps.http.fetch({
          method: 'POST',
          path,
          body,
          signal,
        })
      );
    };

    const dataFrame = fetchDataFrame(
      searchRequest.params.body.query.queries[0].query,
      searchRequest.params.body.df
    );

    // subscribe to dataFrame to see if an error is returned, display a toast message if so
    dataFrame.subscribe((df) => {
      if (!df.body.error) return;
      const jsError = new Error(df.body.error.response);
      this.deps.toasts.addError(jsError, {
        title: i18n.translate('queryEnhancements.sqlQueryError', {
          defaultMessage: 'Could not complete the SQL query',
        }),
        toastMessage: df.body.error.msg,
      });
    });

    return dataFrame;
  }

  public search(request: IOpenSearchDashboardsSearchRequest, options: ISearchOptions) {
    return this.runSearch(request, options.abortSignal, SEARCH_STRATEGY.SQL);
  }
}
