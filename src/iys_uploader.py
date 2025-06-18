import requests
import pandas as pd
import time
import logging
import os
import urllib.parse
from typing import Any, Generator, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IYSConsentUploader:
    def __init__(self):
        self.token_url = "https://api.iys.org.tr/oauth2/token"
        
        # IYS Numaranız ve Marka Kodunuzu buraya girin
        iys_no = 710271
        brand_code = 710271 # Genellikle IYS No ile aynıdır, değilse değiştirin

        # URL'leri dinamik olarak oluştur
        base_sps_url = f"https://api.iys.org.tr/sps/{iys_no}/brands/{brand_code}"
        self.consent_url = f"{base_sps_url}/consents/request"
        self.status_url_template = f"{base_sps_url}/consents/request/{{}}"

        self.username = os.getenv("IYS_USERNAME")
        self.password = os.getenv("IYS_PASSWORD")
        self.access_token = None
        self.brand_code = brand_code

        if not self.username or not self.password:
            raise ValueError("IYS_USERNAME and IYS_PASSWORD must be set as environment variables (Streamlit Secrets or server environment variables).")

    def get_token(self) -> bool:
        """Fetches the OAuth2 token from IYS. Returns True on success, False on failure."""
        logging.info("Attempting to get IYS token...")
        try:
            payload = {
                'grant_type': 'password',
                'username': self.username,
                'password': self.password
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            payload_encoded = urllib.parse.urlencode(payload)
            response = requests.post(self.token_url, data=payload_encoded, headers=headers)
            response.raise_for_status()
            self.access_token = response.json().get('access_token')
            if self.access_token:
                logging.info("Successfully obtained IYS token.")
                return True
            else:
                logging.error("API response did not contain an access_token.")
                return False
        except requests.exceptions.RequestException as e:
            error_details = e.response.text if e.response else "No response from server"
            logging.error(f"API Error during token fetch - {str(e)} | Details: {error_details}")
            return False

    def format_phone_number(self, phone: Any) -> str:
        """Formats the phone number to the required +90 E.164 format."""
        phone_str = str(phone).strip()
        # Remove .0 suffix if it exists (from float conversion)
        if phone_str.endswith('.0'):
            phone_str = phone_str[:-2]

        if not phone_str.startswith('+'):
            if phone_str.startswith('90'):
                phone_str = '+' + phone_str
            # Standard Turkish mobile numbers are 10 digits (e.g., 5xxxxxxxxx)
            elif len(phone_str) == 10:
                phone_str = '+90' + phone_str
            else:
                # Fallback for numbers that might already include country code but no +
                phone_str = '+' + phone_str
        return phone_str

    def add_consents(self, consent_data: list) -> str:
        """Submits a consent request and returns the request ID."""
        if not self.access_token:
            if not self.get_token():
                raise ConnectionError("Failed to authenticate with IYS. Cannot add consents.")

        logging.info(f"Submitting consent request for {len(consent_data)} recipients...")
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.post(self.consent_url, json=consent_data, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        request_id = response_json.get("requestId")
        if not request_id:
            raise ValueError(f"Could not get requestId from IYS. Response: {response_json}")
        logging.info(f"Consent request submitted successfully. Request ID: {request_id}")
        return request_id

    def check_consent_status(self, request_id: str) -> Dict:
        """Checks the status of a previously submitted consent request."""
        if not self.access_token:
            if not self.get_token():
                raise ConnectionError("Failed to authenticate with IYS. Cannot check status.")
        
        status_url = self.status_url_template.format(request_id)
        headers = {'Authorization': f'Bearer {self.access_token}'}
        logging.info(f"Checking status for request {request_id}...")
        response = requests.get(status_url, headers=headers)
        response.raise_for_status()
        return response.json()

    def process_dataframe(self, df: pd.DataFrame) -> Generator[Dict[str, Any], None, None]:
        """Processes a DataFrame and yields status updates."""
        try:
            if not self.get_token():
                yield {'status': 'error', 'message': "IYS kimlik doğrulaması başarısız. Lütfen bilgileri kontrol edin.", 'progress': 0.0}
                return

            yield {'status': 'info', 'message': 'İzin verileri hazırlanıyor...', 'progress': 0.1}

            # Deduplicate based on recipient and type
            original_count = len(df)
            df_deduplicated = df.drop_duplicates(subset=['ALICI', 'IZIN TURU'], keep='last').copy()
            deduplicated_count = len(df_deduplicated)
            if original_count > deduplicated_count:
                removed_count = original_count - deduplicated_count
                yield {'status': 'warning', 'message': f"{removed_count} adet tekrar eden kayıt bulundu ve listeden kaldırıldı.", 'progress': 0.15}

            consent_list = []
            for _, row in df_deduplicated.iterrows():
                # Ensure type and source are strings to prevent errors with empty cells (NaN)
                izin_turu = str(row['IZIN TURU']) if pd.notna(row['IZIN TURU']) else ''
                izin_kaynagi = str(row['IZIN KAYNAGI']) if pd.notna(row['IZIN KAYNAGI']) else ''

                # Skip rows with no permission type
                if not izin_turu:
                    continue

                consent_list.append({
                    "type": izin_turu.upper(),
                    "status": "ONAY" if int(row['ONAY(1)-RET(0)']) == 1 else "RET",
                    "source": izin_kaynagi,
                    "recipient": self.format_phone_number(row['ALICI']),
                    "recipientType": "BIREYSEL",
                    "consentDate": pd.to_datetime(row['IZIN TARIHI'], format='%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                })
            
            if not consent_list:
                yield {'status': 'warning', 'message': 'Yüklenecek geçerli bir kayıt bulunamadı.', 'progress': 1.0}
                return

            yield {'status': 'info', 'message': f"{len(consent_list)} adet izin isteği gönderiliyor...", 'progress': 0.3}
            request_id = self.add_consents(consent_list)
            yield {'status': 'success', 'message': f"İstek başarıyla gönderildi. Talep ID: {request_id}", 'progress': 0.5}

            # Polling for status
            for i in range(12): # Poll for up to 2 minutes (12 * 10s)
                time.sleep(10)
                progress = 0.5 + (i + 1) * (0.5 / 12)
                yield {'status': 'info', 'message': f"Sonuçlar kontrol ediliyor... (Deneme {i+1}/12)", 'progress': progress}
                
                status_result = self.check_consent_status(request_id)
                
                if isinstance(status_result, list) and status_result:
                    # The job is done only if NO items are currently being processed.
                    is_processing = any(item.get("status", "").lower() in ["enqueue", "processing"] for item in status_result)

                    if not is_processing:
                        yield {'status': 'info', 'message': "İşlem tamamlandı, sonuçlar işleniyor...", 'progress': 0.9}
                        
                        success_count = 0
                        failure_count = 0

                        # Create a list of recipients to map API response index to phone number
                        recipient_list = df_deduplicated['ALICI'].tolist()

                        for item in status_result:
                            item_status = item.get("status", "").lower()
                            original_index = item.get('index', -1)
                            
                            recipient = 'Bilinmeyen Alıcı'
                            if 0 <= original_index < len(recipient_list):
                                recipient = self.format_phone_number(recipient_list[original_index])

                            if item_status in ["success", "completed"]:
                                success_count += 1
                            else: # failure or any other error status
                                failure_count += 1
                                error_info = item.get('error', {})
                                error_message = error_info.get('message', 'Bilinmeyen hata.')
                                yield {'status': 'error', 'message': f"Alıcı {recipient} (Sıra #{original_index}) başarısız: {error_message}"}
                        
                        summary_message = f"İşlem tamamlandı. Başarılı: {success_count}, Başarısız: {failure_count}."
                        final_status = 'success' if failure_count == 0 else 'warning'
                        yield {'status': final_status, 'message': summary_message, 'progress': 1.0}
                        yield {'status': 'complete', 'message': 'Tüm işlemler bitti.', 'progress': 1.0}
                        return

            yield {'status': 'warning', 'message': "Sonuç beklenenden uzun sürdü. Lütfen IYS panelinden kontrol edin.", 'progress': 1.0}

        except requests.exceptions.HTTPError as e:
            error_details = e.response.text if e.response is not None else "No details from server."
            logging.error(f"API Error - {str(e)} | Details: {error_details}")
            yield {'status': 'error', 'message': f"API Hatası ({e.response.status_code}): Sunucu gönderilen veriyi geçersiz buldu. Detaylar: {error_details}", 'progress': 1.0}
        except Exception as e:
            logging.error(f"An unexpected error occurred: {str(e)}")
            yield {'status': 'error', 'message': f"Beklenmedik bir hata oluştu: {str(e)}", 'progress': 1.0}
