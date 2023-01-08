#!/usr/bin/env node
import 'source-map-support/register'
import { App } from 'aws-cdk-lib'
import { Hl7DataLakeStack } from '../lib/hl7-data-lake-stack'
import { Hl7ObservabilityStack } from '../lib/hl7-observability-stack'

const app = new App()

const environment = app.node.tryGetContext('environment')
const envContext = app.node.tryGetContext(environment)

if (!envContext) { throw new Error(`Invalid environment: ${environment}`) }
console.log(`DEPLOYING TO ${environment}..`)

const regionalEnv = {env: {region: envContext.region, account: envContext.account}}

new Hl7DataLakeStack(app, `Hl7DataLakeStack-${environment}`, {
  environment,
  hl7LayerArn: envContext['hl7LayerArn'],
  embeddedMetricsLayerArn: envContext['embeddedMetricsLayerArn'],
  otelLayerArn: envContext['otelLayerArn'],
  ...regionalEnv
})

new Hl7ObservabilityStack(app, `Hl7ObservabilityStack-${environment}`, {
  environment,
  ...regionalEnv
})
