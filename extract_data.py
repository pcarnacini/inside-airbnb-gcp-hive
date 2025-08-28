import os
import io
import gzip
import logging
import requests
import yaml
from datetime import datetime
import csv

LOCAL_RAW_DIR = "data/raw"
CONFIG_PATH = os.path.join("config", "cities.yaml")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_directories():
    """Cria diretórios necessários"""
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)
    for subdir in ['listings', 'calendar', 'reviews']:
        os.makedirs(os.path.join(LOCAL_RAW_DIR, subdir), exist_ok=True)

def download_file(city: str, snapshot: str, base_url: str, filename: str):
    """Baixa arquivos da fonte e salva localmente"""
    url = f"{base_url.rstrip('/')}/{snapshot}/data/{filename}"
    logger.info(f"Baixando: {url}")
    
    try:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        
        # Determinar o tipo de arquivo e subdiretório
        if 'listings' in filename.lower():
            subdir = 'listings'
        elif 'calendar' in filename.lower():
            subdir = 'calendar'
        elif 'reviews' in filename.lower():
            subdir = 'reviews'
        else:
            subdir = 'other'
            os.makedirs(os.path.join(LOCAL_RAW_DIR, subdir), exist_ok=True)
        
        # Salvar arquivo original localmente
        local_path = os.path.join(LOCAL_RAW_DIR, subdir, f"{city}_{snapshot}_{filename}")
        
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Arquivo salvo: {local_path}")
        return local_path
        
    except requests.RequestException as e:
        logger.error(f"Falha ao baixar {url}: {e}")
        return None

def extract_compressed_file(file_path: str):
    """Extrai arquivos comprimidos e já aplica limpeza básica"""
    if file_path.endswith('.gz'):
        try:
            # Determinar subdiretório
            filename = os.path.basename(file_path)
            subdir = os.path.basename(os.path.dirname(file_path))
            
            # Ler arquivo comprimido
            with gzip.open(file_path, 'rb') as gz_file:
                content = gz_file.read()
            
            # Salvar CSV descomprimido
            csv_filename = filename[:-3]  # Remove .gz
            csv_path = os.path.join(LOCAL_RAW_DIR, subdir, csv_filename)
            
            # Escrever conteúdo descomprimido
            with open(csv_path, 'wb') as f:
                f.write(content)
            
            # Remover arquivo comprimido original
            os.remove(file_path)
            
            logger.info(f"Descomprimido: {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"Erro ao descomprimir arquivo {file_path}: {e}")
            return None
    
    return file_path

def main():
    ensure_directories()
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    snapshot = cfg.get("snapshot")
    if not snapshot:
        logger.error("snapshot não encontrado no config")
        return

    downloaded_files = []
    
    for city in cfg.get("cities", []):
        name = city.get("name")
        base_url = city.get("base_url", "").rstrip("/")
        if not name or not base_url:
            logger.warning("Pulando entrada inválida de cidade")
            continue
        
        for filename in city.get("files", []):
            file_path = download_file(name, snapshot, base_url, filename)
            if file_path:
                # Extrair se for arquivo comprimido
                extracted_path = extract_compressed_file(file_path)
                if extracted_path:
                    downloaded_files.append(extracted_path)

    logger.info(f"Extração concluída. Arquivos baixados: {len(downloaded_files)}")
    return downloaded_files

if __name__ == "__main__":
    main()