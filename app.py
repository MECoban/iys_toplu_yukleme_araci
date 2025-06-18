import streamlit as st
import pandas as pd
from src.iys_uploader import IYSConsentUploader
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file for local development
# This line will be ignored in Streamlit Cloud where there is no .env file.
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

st.set_page_config(page_title="IYS Toplu İzin Yükleme", layout="wide")

st.title("IYS Toplu İzin Yükleme Servisi")
st.write("Bu araç, CSV formatındaki izin verilerinizi İYS sistemine toplu olarak yüklemenizi sağlar.")
st.markdown("---")


st.header("1. CSV Dosyanızı Yükleyin")
st.write("Lütfen aşağıdaki kolonları içeren CSV dosyanızı yükleyin.")
st.info("""
**Gerekli Kolonlar ve Örnek Format:**
```csv
IZIN TURU,ALICI,ONAY(1)-RET(0),IZIN KAYNAGI,IZIN TARIHI
MESAJ,5459419845,1,HS_WEB,2025-06-20 14:00:00
ARAMA,5467338892,0,HS_FIZIKSEL_ORTAM,2025-06-20 14:05:00
```
**Notlar:**
- `ONAY(1)-RET(0)`: Onay için `1`, ret için `0` kullanın.
- `IZIN TARIHI`: İYS kuralları gereği son 3 iş günü içinde olmalıdır.
""")

uploaded_file = st.file_uploader("Detaylı izin dosyasını (CSV) buraya sürükleyin", type="csv", key="add_uploader")

if uploaded_file:
    try:
        # Ensure the 'ALICI' column is read as a string to prevent it being treated as a number
        df = pd.read_csv(uploaded_file, dtype={'ALICI': str})
        st.header("2. Veri Önizlemesi ve Doğrulama")
        st.dataframe(df.head(), use_container_width=True)
        required_columns = {'ALICI', 'ONAY(1)-RET(0)', 'IZIN TARIHI', 'IZIN TURU', 'IZIN KAYNAGI'}
        
        if not required_columns.issubset(df.columns):
            st.error(f"Hata: Yüklenen CSV dosyasında gerekli olan şu kolonlar eksik: {', '.join(required_columns - set(df.columns))}")
        else:
            st.success("Tüm gerekli kolonlar bulundu.")
            st.header("3. Yüklemeyi Başlatın")
            if st.button("İzinleri İYS'ye Yükle", type="primary", key="add_button"):
                uploader = IYSConsentUploader()
                with st.spinner("İYS'ye bağlanılıyor ve yükleme işlemi başlatılıyor..."):
                    st.subheader("Yükleme Günlüğü")
                    progress_bar = st.progress(0, text="Yükleme durumu")
                    log_area = st.container(height=300)
                    for result in uploader.process_dataframe(df):
                        progress = result.get('progress', 0); message = result.get('message', ''); status = result.get('status', 'info')
                        progress_bar.progress(progress, text=f"İşlem ilerlemesi: {int(progress * 100)}%")
                        if status == 'success': log_area.success(message)
                        elif status == 'error': log_area.error(message)
                        elif status == 'warning': log_area.warning(message)
                        elif status == 'info': log_area.info(message)
                        elif status == 'complete': log_area.info(message); st.balloons()
    except Exception as e:
        st.error(f"Dosya işlenirken bir hata oluştu: {str(e)}") 