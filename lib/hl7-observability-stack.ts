import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib'
import { Construct } from 'constructs'
import { aws_aps as aps } from 'aws-cdk-lib'
import { aws_logs as logs } from 'aws-cdk-lib'
import { aws_grafana as grafana } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib'

export interface Hl7ObservabilityStackProps extends StackProps {
    readonly environment: string
}

export class Hl7ObservabilityStack extends Stack {
    constructor(scope: Construct, id: string, props: Hl7ObservabilityStackProps) {
        super(scope, id, props)

        const namingPrefix = 'hl7-observability'

        const logGroup = new logs.LogGroup(this, 'Hl7ObservabilityLogGroup', {
            logGroupName: `${namingPrefix}-prometheus-${props.environment}`,
            removalPolicy: RemovalPolicy.DESTROY,
        })

        // const prometheusWorkspace =
        new aps.CfnWorkspace(this, 'Hl7ObservabilityPrometheusWorkspace', {
            //alertManagerDefinition: 'alertManagerDefinition',
            alias: namingPrefix,
            loggingConfiguration: {
              logGroupArn: logGroup.logGroupArn,
            },
            tags: [{
              key: 'Name',
              value: namingPrefix,
            }],
          })
        const grafanaRole = new iam.Role(this, 'Hl7DataLakeGrafanaRole', {
            roleName: `hl7-grafana-${props.environment}`,
            assumedBy: new iam.ServicePrincipal('grafana.amazonaws.com'),
            managedPolicies: [{
                managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonPrometheusFullAccess'
            }],
        })
        new grafana.CfnWorkspace(this, 'Hl7ObservabilityGrafanaWorkspace', {
          accountAccessType: 'CURRENT_ACCOUNT',
          authenticationProviders: ['AWS_SSO'],
          name: 'HL7Grafana',
          description: 'Observability example',
          permissionType: 'SERVICE_MANAGED',
          roleArn: grafanaRole.roleArn,
          dataSources: ['PROMETHEUS'],
        });
    }
}
