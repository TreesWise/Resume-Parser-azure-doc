from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Security, Depends
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
import os
import shutil
import json
import tempfile
from datetime import datetime
# from cv_json_docpage import cv_json
from dotenv import load_dotenv
from doc_intelligence_with_formatting import extract_date_fields, update_date_fields, transform_extracted_info, upload_to_blob_storage, reposition_fields, validate_parsed_resume, extract_resume_info, send_to_gpt, replace_values, replace_rank, convert_docx_to_pdf
from rank_map_dict import rank_mapping
from dict_file import mapping_dict
load_dotenv()

app = FastAPI(title="Resume Parser API", version="1.0")

# experience_swap_map = {'6': '3', '3': '4', '4': '7', '5': '8', '7': '5', '8': '6'}
# certificate_swap_map = {'2': '4', '3': '5', '4': '2', '5': '3'}

# Secure API Key Authentication
API_KEY = os.getenv("your_secure_api_key")
API_KEY_NAME = os.getenv("api_key_name")
endpoint = os.getenv("endpoint")
key = os.getenv("key")
model_id = os.getenv("model_id")
container_name = os.getenv("container_name")
connection_string = os.getenv("connection_string")

basic_details_order = [
    "Name", "FirstName", "MiddleName", "LastName", "Nationality", "Gender", 
    "Doa", "Dob", "Address1", "Address2", "Address3", "Address4", "City", 
    "State", "Country", "ZipCode", "EmailId", "MobileNo", "AlternateNo", "Rank"
]

experience_table_order = [
    "VesselName", "VesselType", "Position", "VesselSubType", "Employer", 
    "Flag", "IMO", "FromDt", "ToDt", "others"
]

certificate_table_order = [
    "CertificateNo", "CertificateName", "PlaceOfIssue", "IssuedBy", "DateOfIssue", 
    "DateOfExpiry", "Grade", "Others", "CountryOfIssue"
]

# Define API Key Security
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

    
def verify_api_key(api_key: str = Security(api_key_header)):
    """Validate API Key"""
    if not api_key or api_key != API_KEY:
        raise HTTPException(status_code=403, detail=" Invalid API Key")
    return api_key

import os
import tempfile
import shutil
import json
from datetime import datetime

@app.post("/upload/")
async def upload_file(
    api_key: str = Depends(verify_api_key),  # Enforce API key authentication
    file: UploadFile = File(...), 
    entity: str = Form("")
):
    try:
        # Extract file extension
        suffix = os.path.splitext(file.filename)[-1]
        if suffix not in [".pdf", ".docx"]:
            raise HTTPException(status_code=400, detail="Only PDF and Word documents are allowed")

        # Generate custom filename with timestamp
        timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        temp_file_path = os.path.join(tempfile.gettempdir(), f"{timestamp}{suffix}")

        # Write the uploaded file to the custom temp path
        with open(temp_file_path, "wb") as temp_file:
            shutil.copyfileobj(file.file, temp_file)

        # Handle .docx conversion if needed
        if suffix == ".docx":
            temp_file_path = await convert_docx_to_pdf(temp_file_path)  

        # Extract JSON from document
        extracted_info = extract_resume_info(endpoint, key, model_id, temp_file_path)
        print(extracted_info)
        print("-------------------------------------------------extracted text-------------------------------------------------","\n")

        validation_errors = validate_parsed_resume(extracted_info, temp_file_path, 0.8, container_name, connection_string)
        print(validation_errors)

        # if "Low confidence score" in validation_errors:
        #     with open('output_format.json', 'r') as f:
        #         json_data = json.load(f)
        #         json_data["utc_time_stamp"] = datetime.utcnow().strftime("%d/%m/%Y, %H:%M:%S")
        #     return json_data

        transformed_data = transform_extracted_info(extracted_info)
        print("-----------------------------------------------------------------------------------------------------------------","\n")

        date_fields = extract_date_fields(transformed_data)
        result = send_to_gpt(date_fields)
        mapped_result = update_date_fields(transformed_data, result)
        course_map = replace_values(mapped_result, mapping_dict)
        rank_map = replace_rank(course_map, rank_mapping)

        
        # # swap_values(rank_map["data"]["experience_table"], experience_swap_map)
        # # swap_values(rank_map["data"]["certificate_table"], certificate_swap_map)
        # basic_details = rank_map['data']['basic_details']
        # experience_table = rank_map['data']['experience_table']
        # certificate_table = rank_map['data']['certificate_table']

        # # Reposition columns for each section
        # basic_details = reposition_fields(basic_details, basic_details_order)
        # experience_table = reposition_fields(experience_table, experience_table_order)
        # certificate_table = reposition_fields(certificate_table, certificate_table_order)

        # # Update the input_json with the new order
        # rank_map['data']['basic_details'] = basic_details
        # rank_map['data']['experience_table'] = experience_table
        # rank_map['data']['certificate_table'] = certificate_table

        
        basic_details = rank_map.get('data', {}).get('basic_details', [])
        experience_table = rank_map.get('data', {}).get('experience_table', [])
        certificate_table = rank_map.get('data', {}).get('certificate_table', [])

        # Reposition columns only if the section exists
        if basic_details:
            basic_details = reposition_fields(basic_details, basic_details_order)
        if experience_table:
            experience_table = reposition_fields(experience_table, experience_table_order)
        if certificate_table:
            certificate_table = reposition_fields(certificate_table, certificate_table_order)

        # Update the input_json with the new order (only if they exist)
        rank_map.setdefault('data', {})
        rank_map['data']['basic_details'] = basic_details
        rank_map['data']['experience_table'] = experience_table
        rank_map['data']['certificate_table'] = certificate_table  
        
        return rank_map

    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


