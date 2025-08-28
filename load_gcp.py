import os
import logging
from google.cloud import storage

PROJECT_ID = "planar-alliance-467623-f1"
BUCKET_NAME = "airbnb-data-pedro"
GCS_PREFIX = "inside_airbnb"
LOCAL_PROCESSED_DIR = "data/processed"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_to_gcs(local_path: str, gcs_path: str):
    """Faz upload de arquivo para GCS"""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        blob.upload_from_filename(local_path)
        logger.info(f"Uploaded: gs://{BUCKET_NAME}/{gcs_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {local_path}: {e}")
        return False

def main():
    if not os.path.exists(LOCAL_PROCESSED_DIR):
        logger.error("No processed data found. Run transform script first.")
        return

    uploaded_count = 0
    
    for data_type in ['listings', 'calendar', 'reviews']:
        type_dir = os.path.join(LOCAL_PROCESSED_DIR, data_type)
        
        if os.path.exists(type_dir):
            for file in os.listdir(type_dir):
                if file.endswith('.csv'):
                    local_path = os.path.join(type_dir, file)
                    
                    # Extrair cidade e snapshot do nome do arquivo
                    # Formato: {city}_{snapshot}_{data_type}.csv
                    parts = file.split('_')
                    if len(parts) >= 3:
                        city = parts[0]
                        snapshot = parts[1]
                        
                        # Upload para estrutura de partições do Hive
                        gcs_path = f"{GCS_PREFIX}/{data_type}/city={city}/snapshot_date={snapshot}/{file}"
                        
                        if upload_to_gcs(local_path, gcs_path):
                            uploaded_count += 1
                    else:
                        logger.warning(f"Formato de arquivo inválido: {file}")

    logger.info(f"Load completed. Files uploaded to GCS: {uploaded_count}")
    
if __name__ == "__main__":
    main()