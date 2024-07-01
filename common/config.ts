/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import { schema, TypeOf } from '@osd/config-schema';

export const configSchema = schema.object({
  enabled: schema.boolean({ defaultValue: true }),
  savedObjects: schema.object({
    repo: schema.object({
      index: schema.string({ defaultValue: '.ql-datasources' }),
      hostName: schema.string({ defaultValue: '' }),
      userName: schema.string({ defaultValue: '' }),
      password: schema.string({ defaultValue: '' }),
      port: schema.number({ defaultValue: 0 }),
      max: schema.number({ defaultValue: 100 }),
      idleTimeoutMillis: schema.number({ defaultValue: 10000 }),
    }),
  }),
  queryAssist: schema.object({
    supportedLanguages: schema.arrayOf(
      schema.object({
        language: schema.string(),
        agentConfig: schema.string(),
      }),
      {
        defaultValue: [{ language: 'PPL', agentConfig: 'os_query_assist_ppl' }],
      }
    ),
  }),
});

export type ConfigSchema = TypeOf<typeof configSchema>;
