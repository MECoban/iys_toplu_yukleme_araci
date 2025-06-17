import pandas as pd
import requests
import logging
from typing import List, Dict, Any, Iterator
import urllib.parse
import time
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iys_operations.log'),
        logging.StreamHandler()
    ]
)

# ------- CONFIGURATION -------
TOKEN_URL = "https://api.iys.org.tr/oauth2/token"
# Renamed for clarity
BASE_URL = "https://api.iys.org.tr/sps/710271/brands/710271"
ADD_CONSENTS_URL = f"{BASE_URL}/consents/request"
STATUS_CHECK_URL = f"{BASE_URL}/consents/request/{{requestId}}"

class IYSConsentUploader:
    def __init__(self):
        load_dotenv()
        self.base_url = "https://api.iys.org.tr/oauth2/token"
        self.consent_url = "https://api.iys.org.tr/sps/consents/request"
        self.username = os.getenv("IYS_USERNAME")
        self.password = os.getenv("IYS_PASSWORD")
        self.access_token = None
        self.request_id = None
        self.brand_code = 283296

        if not self.username or not self.password:
            raise ValueError("IYS_USERNAME and IYS_PASSWORD must be set in the .env file.")

        self.token = None
        self.headers = None

    def get_token(self) -> str:
        """Get OAuth2 access token from IYS API."""
        try:
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            payload = {
                'grant_type': 'password',
                'username': self.username,
                'password': self.password
            }
            payload_encoded = urllib.parse.urlencode(payload)
            response = requests.post(TOKEN_URL, data=payload_encoded, headers=headers)
            response.raise_for_status()
            self.token = response.json()['access_token']
            
            self.headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }

            logging.info("Successfully obtained access token")
            return self.token

        except requests.exceptions.RequestException as e:
            logging.error(f"Error obtaining access token: {str(e)}")
            if hasattr(e.response, 'text'):
                logging.error(f"Response content: {e.response.text}")
            raise

    def format_phone_number(self, phone: Any) -> str:
        """Format phone number to include country code."""
        phone_str = str(phone)
        phone = ''.join(filter(str.isdigit, phone_str))
        if not phone.startswith('90'):
            phone = '90' + phone
        return '+' + phone

    def chunk_list(self, lst: List[Any], chunk_size: int = 50) -> List[List[Any]]:
        """Split list into chunks of specified size."""
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
    def add_consents(self, consent_data: list) -> str:
        """Submits a consent request and returns the request ID."""
        if not self.token:
            self.get_token()
        
        logging.info(f"Submitting consent request for {len(consent_data)} recipients...")
        response = requests.post(ADD_CONSENTS_URL, json=consent_data, headers=self.headers)
        response.raise_for_status()
        response_json = response.json()
        request_id = response_json.get("requestId")
        logging.info(f"Request accepted with ID: {request_id}")
        return request_id

    def check_request_status(self, request_id: str) -> Any:
        """
        Polls the status of a bulk request. Returns the final JSON response,
        which could be a dict or a list.
        """
        if not self.token:
            self.get_token()
        
        status_url = STATUS_CHECK_URL.format(requestId=request_id)
        # Poll for up to 60 seconds
        for i in range(12):
            logging.info(f"Checking status for request {request_id} (Attempt {i+1})...")
            response = requests.get(status_url, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            
            # Define all possible "in-progress" statuses
            in_progress_statuses = {"PENDING", "ENQUEUE", "IN_PROGRESS"}

            # Determine if we should keep polling
            keep_polling = False
            if isinstance(result, dict) and result.get("status") in in_progress_statuses:
                keep_polling = True
            elif isinstance(result, list):
                # If we get a list, we must check if ANY item is still processing
                if any(isinstance(item, dict) and item.get('status') in in_progress_statuses for item in result):
                    keep_polling = True

            if keep_polling:
                logging.info(f"Request {request_id} still processing, waiting 5 seconds...")
                time.sleep(5)
            else:
                # If no items are in-progress, we have the final result
                logging.info(f"Final status received for request {request_id}.")
                return result
        
        raise Exception("Request timed out after 60 seconds. The server took too long to process the request.")

    def process_dataframe(self, df: pd.DataFrame) -> Iterator[Dict[str, Any]]:
        """
        Processes a DataFrame to upload consents, yielding progress and results.
        """
        try:
            required_columns = {'ALICI', 'ONAY(1)-RET(0)', 'IZIN TARIHI', 'IZIN TURU', 'IZIN KAYNAGI'}
            if not required_columns.issubset(df.columns):
                raise Exception(f"CSV must contain the following columns: {', '.join(required_columns)}")

            consent_data = []
            for _, row in df.iterrows():
                phone = self.format_phone_number(row['ALICI'])
                consent_date = pd.to_datetime(row["IZIN TARIHI"], format='%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                status = "ONAY" if row["ONAY(1)-RET(0)"] == 1 else "RET"
                
                izin = {
                    "recipient": phone,
                    "type": row["IZIN TURU"],
                    "source": row["IZIN KAYNAGI"],
                    "status": status,
                    "consentDate": consent_date,
                    "recipientType": "BIREYSEL"
                }
                consent_data.append(izin)

            chunks = self.chunk_list(consent_data, 50)
            total_chunks = len(chunks)

            for i, chunk in enumerate(chunks):
                try:
                    request_id = self.add_consents(chunk)
                    yield {'status': 'info', 'progress': (i + 0.5) / total_chunks, 'message': f"Chunk {i+1}/{total_chunks}: Request submitted with ID: {request_id}. Now checking for final status..."}
                    
                    final_result = self.check_request_status(request_id)
                    
                    if isinstance(final_result, dict):
                        # This case is less likely, but we keep it for robustness
                        success_count = final_result.get('completedCount', 0)
                        failed_items = final_result.get('subRequestErrors', [])
                        fail_count = final_result.get('failedCount', len(failed_items))
                        
                        yield {'status': 'success', 'progress': (i + 1) / total_chunks, 'message': f"Chunk {i+1}/{total_chunks}: Processing complete. Success: {success_count}, Failed: {fail_count}."}

                        if fail_count > 0:
                             yield {'status': 'warning', 'progress': (i + 1) / total_chunks, 'message': f"Failed records details: {failed_items}"}
                    
                    elif isinstance(final_result, list):
                        # This is the expected case based on the logs
                        success_count = sum(1 for item in final_result if isinstance(item, dict) and item.get('status') == 'COMPLETED')
                        failed_items = [item for item in final_result if isinstance(item, dict) and item.get('status') != 'COMPLETED']
                        fail_count = len(failed_items)

                        yield {'status': 'success', 'progress': (i + 1) / total_chunks, 'message': f"Chunk {i+1}/{total_chunks}: Processing complete. Success: {success_count}, Failed: {fail_count}."}

                        if fail_count > 0:
                            yield {'status': 'warning', 'progress': (i + 1) / total_chunks, 'message': f"Failed records details: {failed_items}"}
                    
                    else:
                        # Unexpected response type
                        yield {'status': 'error', 'progress': (i + 1) / total_chunks, 'message': f"Chunk {i+1}/{total_chunks}: Unexpected API response type. Details: {final_result}"}

                except requests.exceptions.RequestException as api_error:
                    error_message = f"Chunk {i+1}/{total_chunks}: API Error - {api_error}"
                    if hasattr(api_error.response, 'text'):
                        error_message += f" | Details: {api_error.response.text}"
                    yield {'status': 'error', 'progress': (i + 1) / total_chunks, 'message': error_message}
            
            yield {'status': 'complete', 'progress': 1.0, 'message': 'All chunks processed.'}

        except Exception as e:
            yield {'status': 'error', 'progress': 0, 'message': f"An error occurred during processing: {str(e)}"}
            raise
