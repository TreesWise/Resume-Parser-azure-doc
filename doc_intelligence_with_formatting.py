from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.storage.blob import BlobServiceClient
import json
import os
from datetime import datetime
import json
import tempfile
from fastapi import HTTPException
from docx2pdf import convert


AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

# output_json_path = r"D:\OneDrive - MariApps Marine Solutions Pte.Ltd\liju_resume_parser\Azure_document_intelligence\output_format.json"
with open("output_format.json", "r", encoding="utf-8") as f:
    expected_structure = json.load(f)

def extract_date_fields(structured_json):
    date_fields = {}

    # Extract Dob
    basic_details = structured_json["data"].get("basic_details", [])
    if len(basic_details) > 1:
        for key, value in basic_details[0].items():
            if value == "Dob":
                date_fields[f"basic_details_{value}"] = basic_details[1].get(key)
    
    # Extract dates from experience_table
    if "experience_table" in structured_json["data"]:
        experience_table = structured_json["data"]["experience_table"]
        for key, value in experience_table[0].items():
            if value in ["FromDt", "ToDt"]:
                for i, row in enumerate(experience_table[1:]):
                    date_fields[f"experience_table_{i}_{value}"] = row.get(key)
    
    # Extract dates from certificate_table
    if "certificate_table" in structured_json["data"]:
        certificate_table = structured_json["data"]["certificate_table"]
        for key, value in certificate_table[0].items():
            if value in ["DateOfIssue", "DateOfExpiry"]:
                for i, row in enumerate(certificate_table[1:]):
                    date_fields[f"certificate_table_{i}_{value}"] = row.get(key)

    return date_fields


def update_date_fields(structured_json, corrected_dates):
    """
    Updates the structured JSON with corrected date values using the mapping.
    
    :param structured_json: The original structured JSON.
    :param corrected_dates: Dictionary containing the corrected date values.
    :return: Updated structured JSON with corrected dates if valid JSON, else raises an error.
    """
    # Update Dob in basic_details
    if f"basic_details_Dob" in corrected_dates:
        basic_details = structured_json["data"].get("basic_details", [])
        if len(basic_details) > 1:
            for key, value in basic_details[0].items():
                if value == "Dob":
                    basic_details[1][key] = corrected_dates[f"basic_details_Dob"]

    # Update experience_table dates
    if "experience_table" in structured_json["data"]:
        experience_table = structured_json["data"]["experience_table"]
        for key, value in experience_table[0].items():
            if value in ["FromDt", "ToDt"]:
                for i, row in enumerate(experience_table[1:]):
                    date_key = f"experience_table_{i}_{value}"
                    if date_key in corrected_dates:
                        row[key] = corrected_dates[date_key]

    # Update certificate_table dates
    if "certificate_table" in structured_json["data"]:
        certificate_table = structured_json["data"]["certificate_table"]
        for key, value in certificate_table[0].items():
            if value in ["DateOfIssue", "DateOfExpiry"]:
                for i, row in enumerate(certificate_table[1:]):
                    date_key = f"certificate_table_{i}_{value}"
                    if date_key in corrected_dates:
                        row[key] = corrected_dates[date_key]

    # Validate if the updated structured JSON is still valid JSON
    try:
        json.dumps(structured_json)  # This will raise an error if it's not valid JSON
        return structured_json
    except (TypeError, ValueError) as e:
        raise ValueError(f"Updated JSON is invalid: {e}")

def transform_extracted_info(extracted_info):
    structured_json = {"status": "success", "data": {}, "utc_time_stamp": datetime.utcnow().strftime("%d/%m/%Y, %H:%M:%S")}

    # Basic Details
    fields = extracted_info.get("fields", {})
    basic_details_keys = [
        "Name", "FirstName", "MiddleName", "LastName", "Nationality", "Gender", "Doa", "Dob", 
        "Address1", "Address2", "Address3", "Address4", "City", "State", "Country", "ZipCode", 
        "EmailId", "MobileNo", "AlternateNo", "Rank"
    ]
    
    # Normalize extracted field keys to lowercase for case-insensitive matching
    normalized_fields = {key.lower(): value for key, value in fields.items()}
    
    # Map fields using case-insensitive lookup while keeping original key names
    basic_details_values = [normalized_fields.get(key.lower(), None) for key in basic_details_keys]
    structured_json["data"]["basic_details"] = [
    {str(i): key for i, key in enumerate(basic_details_keys)},
    {str(i): value if value is not None else None for i, value in enumerate(basic_details_values)}]

    # Tables (Experience & Certificate)
    for table in extracted_info.get("tables", []):
        table_name = table["table_name"]
        headers = {str(i): col for i, col in enumerate(table["columns"])}
        rows = [{str(i): (value if value is not None else None) for i, value in enumerate(row)} for row in table["rows"]]
        structured_json["data"][table_name] = [headers] + rows

    return structured_json



def upload_to_blob_storage(file_path, container_name, connection_string):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(file_path))
        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        print(f"File {file_path} successfully uploaded to {container_name}.")
    except Exception as e:
        print(f"Error uploading file to Blob Storage: {e}")

def validate_parsed_resume(extracted_info, file_path, confidence_threshold=0.8, container_name=None, connection_string=None):
    errors = []
    
    # Check confidence score    
    if extracted_info.get("confidence", 1) < confidence_threshold:
        errors.append("Low confidence score")
        
        # Upload file for retraining
        if container_name and connection_string:
            upload_to_blob_storage(file_path, container_name, connection_string)
    
    return errors if errors else ["Resume parsed successfully."]

def extract_resume_info(endpoint, key, model_id, path_to_sample_documents):
    document_intelligence_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    
    with open(path_to_sample_documents, "rb") as f:
        poller = document_intelligence_client.begin_analyze_document(model_id=model_id, body=f)
    result: AnalyzeResult = poller.result()

    extracted_info = {}
    tables = []
    
    if result.documents:
        for idx, document in enumerate(result.documents):
            extracted_info["doc_type"] = document.doc_type
            extracted_info["confidence"] = document.confidence
            extracted_info["model_id"] = result.model_id
            
            if document.fields:
                extracted_info["fields"] = {}
                for name, field in document.fields.items():
                    field_value = field.get("valueString") if field.get("valueString") else field.content
                    extracted_info["fields"][name] = field_value
            
            # Extract table information
            for field_name, field_value in document.fields.items():
                if field_value.type == "array" and field_value.value_array:
                    col_names = []
                    sample_obj = field_value.value_array[0]
                    if "valueObject" in sample_obj:
                        col_names = list(sample_obj["valueObject"].keys())
                    
                    table_rows = []
                    for obj in field_value.value_array:
                        if "valueObject" in obj:
                            value_obj = obj["valueObject"]
                            row_data = [value_obj[col].get("content", None) for col in col_names]
                            table_rows.append(row_data)
                    
                    tables.append({"table_name": field_name, "columns": col_names, "rows": table_rows})
    
    extracted_info["tables"] = tables
    return extracted_info


def send_to_gpt(date_fields):


    prompt = f"""
    Replace all occurrences of dates into the specified format -> DD-MM-YYYY
    
    Here is the data:
    {date_fields}
    
    Ensure the date output strictly adheres to the expected date format.
    Return the output in JSON format.
    """

    client = AzureOpenAI(
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_key=AZURE_OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": """You are an expert AI trained to format all occurrences of dates into DD-MM-YYYY. Make sure to strictly adhere to this format. 
            If a given date cannot be changed to this format, change it to null
            Some of the dates might be given in in english. Try your best to change all these kinds of dates to DD-MM-YYYY format
            """},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0
    )

    res_json = json.loads(response.choices[0].message.content)
    def replace_null_values(d):
        if isinstance(d, dict):
            return {k: replace_null_values(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [replace_null_values(v) for v in d]
        elif d == "null":  # Convert string "null" to None
            return None
        return d
    return replace_null_values(res_json)

def convert_docx_to_pdf(file_path):
    try:
        temp_dir = tempfile.mkdtemp()  # Create a persistent temp directory
        pdf_file_path = os.path.join(temp_dir, "temp.pdf")
        convert(file_path, pdf_file_path)
        # Debugging - Check if file exists
        if not os.path.exists(pdf_file_path):
            raise FileNotFoundError(f"PDF conversion failed, file not found: {pdf_file_path}")
        print(f"PDF successfully created: {pdf_file_path}")  # Confirm PDF creation
        return pdf_file_path  
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DOCX to PDF conversion failed: {str(e)}")

def replace_values(data, mapping):
    if isinstance(data, dict):
        return {mapping.get(key, key): replace_values(value, mapping) for key, value in data.items()}
    elif isinstance(data, list):
        return [replace_values(item, mapping) for item in data]
    elif isinstance(data, str):
        return mapping.get(data, data)  # Replace if found, else keep original
    return data

def replace_rank(json_data, rank_mapping):
    # Convert rank_mapping keys to lowercase for case-insensitive replacement
    rank_mapping = {key.lower(): value for key, value in rank_mapping.items()}

    if isinstance(json_data, dict):
        return {
            key: replace_rank(value, rank_mapping) if key != "2" else  # "2" corresponds to "Position"
            rank_mapping.get(value.lower(), value) if isinstance(value, str) else value
            for key, value in json_data.items()
        }
    elif isinstance(json_data, list):
        return [replace_rank(item, rank_mapping) for item in json_data]
    return json_data
    
