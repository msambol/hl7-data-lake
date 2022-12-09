import * as glue from '@aws-cdk/aws-glue-alpha'

export const columns = [
    {
        name: 'adt_type',
        type: glue.Schema.STRING,
    }, {
        name: 'admission_source',
        type: glue.Schema.STRING,
    }, {
        name: 'attending_doctor',
        type: glue.Schema.STRING,
    }, {
        name: 'date_of_birth',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_code_01',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_code_02',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_code_03',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_code_04',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_code_05',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_description_01',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_description_02',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_description_03',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_description_04',
        type: glue.Schema.STRING,
    }, {
        name: 'diagnosis_description_05',
        type: glue.Schema.STRING,
    }, {
        name: 'discharge_date',
        type: glue.Schema.STRING,
    },  {
        name: 'discharge_disposition',
        type: glue.Schema.STRING,
    },  {
        name: 'encounter_date',
        type: glue.Schema.STRING,
    },  {
        name: 'encounter_number',
        type: glue.Schema.STRING,
    },  {
        name: 'ethnicity',
        type: glue.Schema.STRING,
    },  {
        name: 'financial_class',
        type: glue.Schema.STRING,
    },  {
        name: 'gender',
        type: glue.Schema.STRING,
    }, {
        name: 'insurance_name_1',
        type: glue.Schema.STRING,
    }, {
        name: 'insurance_name_2',
        type: glue.Schema.STRING,
    }, {
        name: 'first_name',
        type: glue.Schema.STRING,
    },  {
        name: 'last_name',
        type: glue.Schema.STRING,
    }, {
        name: 'patient_class',
        type: glue.Schema.STRING,
    },  {
        name: 'patient_identifier',
        type: glue.Schema.STRING,
    },  {
        name: 'race',
        type: glue.Schema.STRING,
    },  {
        name: 'sending_facility',
        type: glue.Schema.STRING,
    },  {
        name: 'state',
        type: glue.Schema.STRING,
    },  {
        name: 'zip_code',
        type: glue.Schema.STRING,
    }, {
        name: 'processed_date',
        type: glue.Schema.DATE,
    }, {
        name: 'filename',
        type: glue.Schema.STRING,
    }
]

export const partitions = [
    {
        name: 'source_facility',
        type: glue.Schema.STRING,
    }, {
        name: 'transaction_date',
        type: glue.Schema.DATE,
    }
]
