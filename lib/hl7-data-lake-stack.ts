import { Stack, StackProps, Duration, RemovalPolicy } from 'aws-cdk-lib'
import { Construct } from 'constructs'
import { aws_cloudwatch as cw } from 'aws-cdk-lib'
import { aws_iam as iam } from 'aws-cdk-lib'
import { aws_lambda as lambda } from 'aws-cdk-lib'
import { aws_logs as logs } from 'aws-cdk-lib'
import { aws_sns as sns } from 'aws-cdk-lib'
import { aws_sns_subscriptions as subscriptions } from 'aws-cdk-lib'
import { aws_sqs as sqs } from 'aws-cdk-lib'
import { aws_s3 as s3 } from 'aws-cdk-lib'
import { aws_s3_notifications as s3n } from 'aws-cdk-lib'
import { aws_kinesisfirehose as kinesisfirehose } from 'aws-cdk-lib'
import { aws_lambda_event_sources as lambdaEventSources } from 'aws-cdk-lib'
import * as lambdaPython from '@aws-cdk/aws-lambda-python-alpha'
import * as glue from '@aws-cdk/aws-glue-alpha'
import * as path from 'path'

import { columns, partitions } from "../schema/schema"
import { CloudWatchEncryptionMode } from '@aws-cdk/aws-glue-alpha'
import { Unit } from 'aws-cdk-lib/aws-cloudwatch'

export interface Hl7DatalakeStackProps extends StackProps {
    readonly environment: string
    readonly hl7LayerArn: string
    readonly embeddedMetricsLayerArn: string
    readonly otelLayerArn: string
}

export class Hl7DataLakeStack extends Stack {
    constructor(scope: Construct, id: string, props: Hl7DatalakeStackProps) {

        super(scope, id, props)

        const namingPrefix = 'hl7-data-lake'
        const resultPrefixParquet = 'processed_parquet'
        const hl7Layer = lambda.LayerVersion.fromLayerVersionArn(this, 'Hl7DataLakeHl7Layer', props.hl7LayerArn)
        const embeddedMetricsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'EmbeddedMetricsLayer', props.embeddedMetricsLayerArn)
        const otelLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'OtelLayer', props.otelLayerArn)

        const ingestionTopic = new sns.Topic(this, 'Hl7DataLakeIngestionTopic', {
            topicName: `${namingPrefix}-ingestion-${props.environment}`,
            displayName: `${namingPrefix}-ingestion-${props.environment}`
          })

        const dataBucket = new s3.Bucket(this, 'Hl7DataLakeDataBucket', {
            bucketName: `${namingPrefix}-${props.environment}-${this.account}`,
            encryption: s3.BucketEncryption.S3_MANAGED,
        })

        const dataDlq = new sqs.Queue(this, 'Hl7DataLakeDataDlq', {
            queueName: `${namingPrefix}-dlq-${props.environment}`,
            retentionPeriod: Duration.days(14),
            encryption: sqs.QueueEncryption.SQS_MANAGED,
            dataKeyReuse: Duration.hours(4),
        })

        const dataQueue = new sqs.Queue(this, 'Hl7DataLakeQueue', {
            queueName: `${namingPrefix}-${props.environment}`,
            retentionPeriod: Duration.days(14),
            visibilityTimeout: Duration.minutes(3),
            encryption: sqs.QueueEncryption.SQS_MANAGED,
            receiveMessageWaitTime: Duration.seconds(20),
            deadLetterQueue: {
                queue: dataDlq,
                maxReceiveCount: 1,
            }
        })

        // S3 --> SNS --> SQS
        dataBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3n.SnsDestination(ingestionTopic), {prefix: 'raw/'})
        ingestionTopic.addSubscription(new subscriptions.SqsSubscription(dataQueue))

        const glueDatabase = new glue.Database(this, 'Hl7DataLakeGlueDatabase', {
            databaseName: `${namingPrefix.replaceAll('-', '_')}_db_${props.environment}`,
        })

        const glueTable = new glue.Table(this, 'Hl7DataLakeGlueTable', {
            database: glueDatabase,
            bucket: dataBucket,
            s3Prefix: `${resultPrefixParquet}/`,
            tableName: `${namingPrefix.replaceAll('-', '_')}_${props.environment}`,
            columns: columns,
            partitionKeys: partitions,
            dataFormat: glue.DataFormat.PARQUET,
            enablePartitionFiltering: true,
        })

        const logGroup = new logs.LogGroup(this, 'Hl7DataLakeLogGroup', {
            logGroupName: `/aws/firehose/${namingPrefix}-logs-${props.environment}`,
            removalPolicy: RemovalPolicy.DESTROY,
        })
        const logStream = new logs.LogStream(this, 'Hl7DataLakeLogStream', {
            logGroup: logGroup,
            logStreamName: `${namingPrefix}-firehose-logs-${props.environment}`,
            removalPolicy: RemovalPolicy.DESTROY,
          })
        const firehoseRole = new iam.Role(this, 'Hl7DataLakeFirehoseRole', {
            roleName: `${namingPrefix}-firehose-${props.environment}`,
            assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com')
        })
        dataBucket.grantReadWrite(firehoseRole)
        logGroup.grantWrite(firehoseRole)
        firehoseRole.addToPrincipalPolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            resources: [glueDatabase.databaseArn, glueDatabase.catalogArn, glueTable.tableArn],
            actions: ['glue:GetTableVersions'],
        }))

        const deliveryStream = new kinesisfirehose.CfnDeliveryStream(this, 'Hl7DataLakeFirehose', {
            deliveryStreamName: `${namingPrefix}-${props.environment}`,
            deliveryStreamEncryptionConfigurationInput: {
                keyType: 'AWS_OWNED_CMK',
            },
            extendedS3DestinationConfiguration: {
              cloudWatchLoggingOptions: {
                enabled: true,
                logGroupName: logGroup.logGroupName,
                logStreamName: logStream.logStreamName,
              },
              bucketArn: dataBucket.bucketArn,
              roleArn: firehoseRole.roleArn,
              prefix: `${resultPrefixParquet}/source_facility=!{partitionKeyFromQuery:source_facility}/transaction_date=!{partitionKeyFromQuery:transaction_date}/`,
              errorOutputPrefix: `${resultPrefixParquet}_errors/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:mm}/!{timestamp:dd}`,
              bufferingHints: {
                intervalInSeconds: 600,
                sizeInMBs: 128,
              },
              dynamicPartitioningConfiguration: {
                enabled: true,
              },
              dataFormatConversionConfiguration: {
                enabled: true,
                inputFormatConfiguration: {deserializer: { openXJsonSerDe: {} }},
                outputFormatConfiguration: {serializer: { parquetSerDe: {} }},
                schemaConfiguration: {
                    databaseName: glueDatabase.databaseName,
                    tableName: glueTable.tableName,
                    roleArn: firehoseRole.roleArn,
                }
              },
              processingConfiguration: {
                enabled: true,
                processors: [
                  {
                    type: 'MetadataExtraction',
                    parameters: [
                      {
                        parameterName: 'MetadataExtractionQuery',
                        parameterValue: '{source_facility: .partitions.source_facility, transaction_date: .partitions.transaction_date}',
                      },
                      {
                        parameterName: 'JsonParsingEngine',
                        parameterValue: 'JQ-1.6',
                      },
                    ],
                  },
                ],
              },
          },
        })
        // eventually consistency, need to wait a sec for the role to be ready
        deliveryStream.node.addDependency(firehoseRole)

        const hl7Lambda = new lambdaPython.PythonFunction(this, 'Hl7DataLakeHl7ParserLambda', {
            functionName: `${namingPrefix}-parser-${props.environment}`,
            description: 'Polls SQS, parses HL7, and writes JSON to Firehose & S3',
            entry: path.join(__dirname, '..', 'lambdas'),
            runtime: lambda.Runtime.PYTHON_3_9,
            index: 'hl7_parser.py',
            handler: 'handler',
            timeout: Duration.minutes(1),
            memorySize: 256,
            retryAttempts: 0,
            reservedConcurrentExecutions: 150,
            tracing: lambda.Tracing.ACTIVE,
            environment: {
                ENVIRONMENT: props.environment,
                DATA_BUCKET: dataBucket.bucketName,
                STREAM_NAME: deliveryStream.deliveryStreamName || '',
                OPENTELEMETRY_COLLECTOR_CONFIG_FILE: '/var/task/collector.yaml',
                AWS_LAMBDA_EXEC_WRAPPER: '/opt/otel-instrument',
                NAMING_PREFIX: namingPrefix,
            },
            layers: [hl7Layer, embeddedMetricsLayer, otelLayer],
        })
        dataBucket.grantReadWrite(hl7Lambda)
        hl7Lambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            resources: [`arn:aws:firehose:${this.region}:${this.account}:deliverystream/${deliveryStream.deliveryStreamName}`],
            actions: ['firehose:PutRecord*'],
        }))
        hl7Lambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: ['*'],
          actions: [
            'aps:RemoteWrite',
            'aps:GetSeries',
            'aps:GetLabels',
            'aps:GetMetricMetadata',
          ]
        }))
        hl7Lambda.addEventSource(new lambdaEventSources.SqsEventSource(dataQueue, {
            batchSize: 1,
        }))

        //
        // OBSERVABILITY 
        //
        // Example 1: Using CloudWatch Logs metric filters
        const metricOneSuccess = 'Example_1_Success'
        const metricSucess = new cw.Metric({
          namespace: namingPrefix,
          metricName: metricOneSuccess,
          period: Duration.minutes(5),
          statistic: 'sum',
          unit: Unit.COUNT,
          dimensionsMap: {
            'LambdaFunctionName': hl7Lambda.functionName
          }
        })
        new logs.MetricFilter(this, 'MetricsExample1Success', {
          logGroup: hl7Lambda.logGroup,
          metricNamespace: namingPrefix,
          metricName: metricOneSuccess,
          filterPattern: logs.FilterPattern.stringValue('$.result', '=', 'SUCCESS'),
          metricValue: '1',
          dimensions: {
            'LambdaFunctionName': '$.lambdaFunctionName'
          }
        })
        const metricOneFailure = 'Example_1_Failure'
        const metricFailures = new cw.Metric({
          namespace: namingPrefix,
          metricName: metricOneFailure,
          period: Duration.minutes(5),
          statistic: 'sum',
          unit: Unit.COUNT,
          dimensionsMap: {
            'LambdaFunctionName': hl7Lambda.functionName
          }
        })
        new logs.MetricFilter(this, 'MetricsExample1Failure', {
          logGroup: hl7Lambda.logGroup,
          metricNamespace: namingPrefix,
          metricName: metricOneFailure,
          filterPattern: logs.FilterPattern.stringValue('$.result', '=', 'FAILED'),
          metricValue: '1',
          dimensions: {
            'LambdaFunctionName': '$.lambdaFunctionName'
          }
        })

        const metricTotalMessages = 'Total_HL7_Messages_Processed'
        const metricTotal = new cw.Metric({
          namespace: namingPrefix,
          metricName: metricTotalMessages,
          period: Duration.minutes(5),
          statistic: 'sum',
          unit: Unit.COUNT,
          dimensionsMap: {
            'LambdaFunctionName': hl7Lambda.functionName
          }
        })

        const metricFailurePercentage = new cw.MathExpression({
          label: 'HL7_Processing_Failure_Percentage',
          period: Duration.minutes(5),
          expression: "100 * (m1/m2)",
          usingMetrics: {
            m1: metricFailures,
            m2: metricTotal,
          }
        })

        new cw.Alarm(this, "Hl7FailureAlarm", {
          metric: metricFailurePercentage,
          alarmName: "HL7 Failure Percentage",
          alarmDescription: "HL7 Failure Percentage",
          datapointsToAlarm: 1,
          evaluationPeriods: 1,
          threshold: 5,
          comparisonOperator: cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
          treatMissingData: cw.TreatMissingData.IGNORE,
        })
    }
}
