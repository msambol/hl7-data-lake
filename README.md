# hl7-data-lake

* [Building an HL7 Data Lake](https://www.michaelsambol.com/blogs/building_an_hl7_date_lake.html)
* [Observability with Amazon CloudWatch, AWS X-Ray, Prometheus, and Grafana](https://www.michaelsambol.com/blogs/observability.html)

## Architecture

![HL7 Data Lake](images/HL7_Data_Lake.png)

## Deploy 

Software versions:
```
❯ node --version
v18.6.0

❯ python --version
Python 3.9.6

❯ cdk --version
2.55.0
```

Install dependencies:
```
npm install
```

You need Docker installed and running to package and deploy the Lambda function.

Create a Lambda layer with the [HL7 Python package](https://pypi.org/project/hl7/) and drop the ARN in `cdk.context.json`. The zip is included in this repo.
```
aws lambda publish-layer-version --layer-name hl7 --zip-file fileb://layers/hl7.zip --compatible-runtimes python3.9 --description "Parsing messages of Health Level 7 (HL7) version 2.x into Python objects"
```

Create a Lambda layer with the [AWS Embedded Metrics package](https://pypi.org/project/aws-embedded-metrics/) and drop the ARN in `cdk.context.json`. The zip is included in this repo.
```
aws lambda publish-layer-version --layer-name aws_embedded_metrics --zip-file fileb://layers/aws_embedded_metrics.zip --compatible-runtimes python3.9 --description "Amazon CloudWatch Embedded Metric Format Client Library"
```

Deploy stack:
```
// dev
cdk deploy --context environment=dev Hl7DataLakeStack-dev
cdk deploy --context environment=dev Hl7ObservabilityStack-dev

// add additional environments if desired
```

## Test

`lambdas/test_hl7_parser.py` is a simple way to test the Lambda function locally without saving any PHI to disk (assumes you have AWS creds set up). 
Place the sample HL7 file in S3 and replace the bucket name in `test/event_1.json` (search for `CHANGE_ME`). Then run:

```
export STREAM_NAME=hl7-data-lake-dev
aws s3 cp test/sample_adt.hl7 s3://<BUCKET>/test/sample_adt.hl7
cd lambdas/
python test_hl7_parser.py
```

You can also drop files in the S3 bucket created by the CDK within the `raw/` prefix, which triggers the process in AWS:

```
aws s3 cp test/sample_adt.hl7 s3://<BUCKET>/raw/sample_adt.hl7
```
