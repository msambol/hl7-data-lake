# These are the values I chose to extract. Amend this however you see fit!

class ParsedHL7(): 
    def __init__(self):
        self.adt_type = ''
        self.admission_source = ''
        self.attending_doctor = ''
        self.date_of_birth = ''

        self.diagnosis_code_01 = ''
        self.diagnosis_code_02 = ''
        self.diagnosis_code_03 = ''
        self.diagnosis_code_04 = ''
        self.diagnosis_code_05 = ''

        self.diagnosis_description_01 = ''
        self.diagnosis_description_02 = ''
        self.diagnosis_description_03 = ''
        self.diagnosis_description_04 = ''
        self.diagnosis_description_05 = ''

        self.discharge_date = ''
        self.discharge_disposition = ''
        self.encounter_date = ''
        self.encounter_number = ''
        self.ethnicity = ''
        self.financial_class = ''
        self.gender = ''
        self.insurance_name_1 = ''
        self.insurance_name_2 = ''
        self.first_name = ''
        self.last_name = ''
        self.patient_class = ''
        self.patient_identifier = ''
        self.race = ''
        self.sending_facility = ''
        self.source_facility = ''
        self.state = ''
        self.transaction_date = ''
        self.zip_code = ''

        self.filename = ''
        self.processed_date = ''
