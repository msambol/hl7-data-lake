import boto3
import datetime
import hl7
import json
import os
#from opentelemetry import _metrics
from aws_embedded_metrics import metric_scope

from ParsedHL7 import *

# boto3
s3 = boto3.client('s3')
s3r = boto3.resource('s3')
firehose = boto3.client('firehose')

# globals
DEBUG = False # keep False with live data, don't want PHI leak
STREAM_NAME = os.environ.get('STREAM_NAME', '')
PROCESSED_JSON_PREFIX = 'processed_json'
PROCESSED_HL7_PREFIX = 'processed_hl7'
NAMING_PREFIX = os.environ.get('NAMING_PREFIX', '')

# metrics
SUCCESS = '[SUCCESS] HL7 message processed'
FAILED = '[FAILED] Failure during HL7 message processing'

def parse_hl7(hl7_body, filename):
    try:
        hl7_string = hl7_body.replace('\n', '')
        parsed_hl7 = ParsedHL7()
        h = hl7.parse(hl7_string)
    except Exception as e:
        raise ValueError(f'[FAILED] Failed to parse HL7 string: {e}')

    # HL7 data is notoriously bad. I'm wrapping all the extractions in try blocks.
    # If there are certain values that you require, you can raise errors.
    # You may also have to adjust some of your indexing based on your HL7 sources.
    
    try:
        parsed_hl7.adt_type = f'{h.segments("MSH")[0][9][0][0][0]}^{h.segments("MSH")[0][9][0][1][0]}'
    except:
        pass
    try:
        # depending on the source of the HL7, sometimes you need to drill further. Messy!
        source = h.segments("PV1")[0][14][0]
        parsed_hl7.admission_source = source if isinstance(source, str) else f'{h.segments("PV1")[0][14][0][0]}^{h.segments("PV1")[0][14][0][1]}'
    except:
        pass
    try:
        parsed_hl7.attending_doctor = f'{h.segments("PV1")[0][7][0][2]} {h.segments("PV1")[0][7][0][1]}'
    except:
        pass
    try:
        parsed_hl7.date_of_birth = h.segments("PID")[0][7][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_code_01 = h.segments("DG1")[0][3][0][0][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_code_02 = h.segments("DG1")[1][3][0][0][0]
    except:
        pass 
    try:
        parsed_hl7.diagnosis_code_03 = h.segments("DG1")[2][3][0][0][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_code_04 = h.segments("DG1")[3][3][0][0][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_code_05 = h.segments("DG1")[4][3][0][0][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_description_01 = h.segments("DG1")[0][3][0][1][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_description_02 = h.segments("DG1")[1][3][0][1][0]
    except:
        pass 
    try:
        parsed_hl7.diagnosis_description_03 = h.segments("DG1")[2][3][0][1][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_description_04 = h.segments("DG1")[3][3][0][1][0]
    except:
        pass
    try:
        parsed_hl7.diagnosis_description_05 = h.segments("DG1")[4][3][0][1][0]
    except:
        pass
    try:
        parsed_hl7.discharge_date = h.segments("PV1")[0][45][0]
    except:
        pass
    try:
        dd = h.segments("PV1")[0][36][0]
        parsed_hl7.discharge_disposition = dd if isinstance(dd, str) else h.segments("PV1")[0][36][0][0][0]
    except:
        pass
    try:
        parsed_hl7.encounter_date = h.segments("PV1")[0][44][0]
    except:
        pass
    try:
        en = h.segments("PV1")[0][19][0]
        parsed_hl7.encounter_number = en if isinstance(en, str) else h.segments("PV1")[0][19][0][0][0]
    except:
        pass
    try:
        eth = h.segments("PID")[0][22][0]
        parsed_hl7.ethnicity = eth if isinstance(eth, str) else f'{h.segments("PID")[0][22][0][0]}^{h.segments("PID")[0][22][0][1]}'
    except:
        pass
    try:
        parsed_hl7.financial_class = h.segments("PV1")[0][20][0]
    except:
        pass
    try:
        parsed_hl7.gender = h.segments('PID')[0][8][0][0][0]
    except:
        pass
    try:
        ins = h.segments('IN1')[0][4][0]
        parsed_hl7.insurance_name_1 = ins if isinstance(ins, str) else h.segments('IN1')[0][4][0][0][0]
    except:
        pass
    try:
        ins = h.segments('IN2')[0][4][0]
        parsed_hl7.insurance_name_2 = ins if isinstance(ins, str) else h.segments('IN2')[0][4][0][0][0]
    except:
        pass
    try:
        parsed_hl7.first_name = h.segments('PID')[0][5][0][1][0]
    except:
        pass
    try:
        parsed_hl7.last_name = h.segments('PID')[0][5][0][0][0]
    except:
        pass
    try:
        c = h.segments("PV1")[0][2][0]
        parsed_hl7.patient_class = c if isinstance(c, str) else h.segments("PV1")[0][2][0][0][0]
    except:
        pass
    try:
        id = h.segments('PID')[0][2][0]
        parsed_hl7.patient_identifier = id if isinstance(id, str) else h.segments("PID")[0][2][0][0][0]

        if parsed_hl7.patient_identifier == "":
            parsed_hl7.patient_identifier = h.segments('PID')[0][3][0][0][0]
    except:
        pass
    try:
        race = h.segments("PID")[0][10][0]
        parsed_hl7.race = race if isinstance(race, str) else f'{h.segments("PID")[0][10][0][0]}^{h.segments("PID")[0][10][0][1]}'
    except:
        pass
    try:
        parsed_hl7.sending_facility = h.segments('MSH')[0][4][0][0][0]
    except:
        pass
    try:
        parsed_hl7.source_facility = h.segments('MSH')[0][4][0][1][0]
    except:
        pass
    try:
        parsed_hl7.state = h.segments('PID')[0][11][0][3][0]
    except:
        pass
    try:
        parsed_hl7.transaction_date = h.segments('MSH')[0][7][0]
    except:
        pass
    try:
        parsed_hl7.zip_code = h.segments('PID')[0][11][0][4][0]
    except:
        pass
    
    parsed_hl7.filename = filename
    # isoformat works with the Firehose deserializer which converts to date type
    parsed_hl7.processed_date = datetime.datetime.now().isoformat()

    if DEBUG:
        print(json.dumps(parsed_hl7.__dict__))

    # return dictionary, not object
    return parsed_hl7.__dict__


def partition_date(transaction_date):
    year = transaction_date[0:4]
    month = transaction_date[4:6]
    day = transaction_date[6:8]

    # check for bad data and confirm these values are digits
    if year.isdigit() and month.isdigit() and day.isdigit():
        return f'{year}-{month}-{day}'
    else:
        # dummy date
        return '9999-12-31'


@metric_scope
def handler(event, context, metrics):
    try:
        for record in event.get('Records', []):
            body = json.loads(record.get('body', {}))
            message = json.loads(body.get('Message', {}))
            
            for record in message.get('Records', []):
                # track messages processed so we can count error rate
                metrics.put_metric('Total_HL7_Messages_Processed', 1, 'Count')

                bucket = record.get('s3').get('bucket').get('name')
                key = record.get('s3').get('object').get('key')
                filename = key.split("/")[-1]
                print(f'Bucket: {bucket}, Key: {key}')
                
                try:
                    bucket_object = s3r.Bucket(bucket)
                    s3_object = bucket_object.Object(key)
                    s3_object_body = s3_object.get()['Body'].read().decode('utf-8')
                except Exception as e:
                    raise Exception(f'[FAILED] Failed reading object body from S3: {e}')

                parsed_hl7 = parse_hl7(s3_object_body, filename)

                # Partitions for S3 prefixing
                parsed_hl7['partitions'] = {}
                source_facility = parsed_hl7['partitions']['source_facility'] = parsed_hl7['source_facility']
                transaction_date = parsed_hl7['partitions']['transaction_date'] = partition_date(parsed_hl7['transaction_date'])
                print(f'Partitions: {json.dumps(parsed_hl7["partitions"])}')


                #################
                # OBSERVABILITY #
                # -- failure -- #
                #################

                # force a failure if first name == "NONAME"
                if parsed_hl7['first_name'] == "NONAME":
                    # Example 1: Using CloudWatch Logs metric filters
                    failed = {
                        'result': 'FAILED',
                        'message': FAILED,
                        'lambdaFunctionName': context.function_name,
                    }
                    print(json.dumps(failed))

                    # Example 2: Embedded metric format
                    metrics.set_namespace(NAMING_PREFIX)
                    metrics.set_dimensions({'LambdaFunctionName': context.function_name})
                    metrics.put_metric('Example_2_Failure', 1, 'Count')

                    continue

                try:
                    # need parsed_hl7['partitions'] for dynamic partitioning in Firehose
                    firehose.put_record(
                        DeliveryStreamName=STREAM_NAME,
                        Record={'Data': json.dumps(parsed_hl7)}
                    )
                    print('Record sent to Firehose!')
                except Exception as e:
                    raise Exception(f'[FAILED] Failed putting record into Firehose: {e}')
                
                try:
                    # don't need partitions in the stored json
                    del parsed_hl7['partitions'] 
                    json_object = s3r.Object(bucket, f'{PROCESSED_JSON_PREFIX}/source_facility={source_facility}/transaction_date={transaction_date}/{filename}.json')
                    json_object.put(Body=json.dumps(parsed_hl7).encode('UTF-8'))
                    print('JSON file saved in processed S3 location!')
                except Exception as e:
                    raise Exception(f'[FAILED] Failed putting json into S3: {e}')
                
                try:
                    # move the file to a processed location
                    copy_source = {'Bucket': bucket, 'Key': key}
                    s3r.Bucket(bucket).copy(copy_source, f'{PROCESSED_HL7_PREFIX}/{filename}')
                    print('Copied HL7 file to processed location!')
                except Exception as e:
                    raise Exception(f'[FAILED] Failed copying HL7 to processed S3 location: {e}')

                try:
                    # remove file from original S3 location (this helps gauge how many are left for processing)
                    s3r.Object(bucket, key).delete()
                    print('Deleted HL7 file from original S3 location!')
                except Exception as e:
                    raise Exception(f'[FAILED] Failed deleting file from original S3 location: {e}')
                    
                # TODO: figure this out later
                # meter = _metrics.get_meter("hl7")
                # counter = meter.create_counter(
                #     name="processed_hl7", description="Hl7 message processed", unit="1",
                # )
                # counter.add(1)

                #################
                # OBSERVABILITY #
                # -- success -- #
                #################

                # Example 1: Using CloudWatch Logs metric filters
                success = {
                    'result': 'SUCCESS',
                    'message': SUCCESS,
                    'lambdaFunctionName': context.function_name,
                }
                print(json.dumps(success))

                # Example 2: Embedded metric format
                metrics.set_namespace(NAMING_PREFIX)
                metrics.set_dimensions({'LambdaFunctionName': context.function_name})
                metrics.put_metric('Example_2_Success', 1, 'Count')

    except Exception as e:
        raise Exception(f'[FAILED] Failed retrieving data from record: {e}')
        
    return True
