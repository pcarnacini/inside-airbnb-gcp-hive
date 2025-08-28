import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import csv
import re

LOCAL_RAW_DIR = "data/raw"
LOCAL_PROCESSED_DIR = "data/processed"
CONFIG_PATH = os.path.join("config", "cities.yaml")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_directories():
    """Cria diretórios necessários"""
    os.makedirs(LOCAL_PROCESSED_DIR, exist_ok=True)
    for subdir in ['listings', 'calendar', 'reviews']:
        os.makedirs(os.path.join(LOCAL_PROCESSED_DIR, subdir), exist_ok=True)

def remove_urls(text):
    """Remove URLs de textos"""
    if pd.isna(text):
        return text
    text_str = str(text)
    # Remove URLs
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    text_str = url_pattern.sub('', text_str)
    return text_str

def clean_text_field(text):
    """Limpa campos de texto que podem conter vírgulas problemáticas"""
    if pd.isna(text):
        return text
    
    text_str = str(text)
    
    # Remove URLs primeiro
    text_str = remove_urls(text_str)
    
    # Remove quebras de linha e tabs que podem causar problemas
    text_str = text_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Escapa aspas duplas se existirem (padrão CSV)
    text_str = text_str.replace('"', '""')
    
    # Remove múltiplos espaços
    text_str = re.sub(r'\s+', ' ', text_str).strip()
    
    return text_str

def clean_price(price_str):
    """Limpa e converte preço para numérico"""
    if pd.isna(price_str):
        return np.nan
    try:
        # Remove $, vírgulas e espaços
        cleaned = str(price_str).replace('$', '').replace(',', '').strip()
        return float(cleaned) if cleaned else np.nan
    except:
        return np.nan

def read_csv_safely(file_path):
    """Lê CSV com tratamento robusto para campos com vírgulas"""
    try:
        # Primeira tentativa: leitura normal
        return pd.read_csv(file_path, low_memory=False, quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        logger.warning(f"Primeira tentativa falhou: {e}. Tentando com engine python...")
        try:
            # Segunda tentativa: com engine python
            return pd.read_csv(file_path, engine='python', quoting=csv.QUOTE_ALL)
        except Exception as e2:
            logger.warning(f"Segunda tentativa falhou: {e2}. Tentando tratamento manual...")
            # Terceira tentativa: leitura linha por linha
            return read_csv_manual(file_path)

def read_csv_manual(file_path):
    """Leitura manual de CSV para casos problemáticos"""
    data = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_ALL)
        header = next(reader)
        for row in reader:
            # Garantir que a linha tenha o mesmo número de colunas que o header
            if len(row) != len(header):
                # Tentar corrigir linhas problemáticas
                corrected_row = []
                in_quotes = False
                current_field = ""
                
                for field in row:
                    if in_quotes:
                        current_field += "," + field if current_field else field
                        if field.endswith('"') and not field.endswith('""'):
                            in_quotes = False
                            corrected_row.append(current_field)
                            current_field = ""
                    else:
                        if field.startswith('"') and not field.endswith('"'):
                            in_quotes = True
                            current_field = field
                        else:
                            corrected_row.append(field)
                
                if in_quotes and current_field:
                    corrected_row.append(current_field)
                
                row = corrected_row
            
            # Se ainda não bater, truncar ou preencher
            if len(row) > len(header):
                row = row[:len(header)]
            elif len(row) < len(header):
                row = row + [''] * (len(header) - len(row))
            
            data.append(row)
    
    return pd.DataFrame(data, columns=header)

def transform_listings(df: pd.DataFrame, city: str, snapshot: str) -> pd.DataFrame:
    """Transformações para dados de listings"""
    # Limpeza básica
    df = df.replace(['', 'N/A', 'NaN', 'null', 'NULL'], np.nan)
    
    # Lista de colunas de texto que podem conter vírgulas
    text_cols = [
        'id', 'name', 'host_id', 'host_name', 'neighbourhood_cleansed', 
        'room_type', 'property_type', 'description', 'summary', 'space',
        'neighborhood_overview', 'notes', 'transit', 'access', 'interaction', 
        'house_rules', 'host_about', 'host_response_time', 'listing_url',
        'host_url', 'picture_url', 'host_thumbnail_url', 'host_picture_url',
        'host_verifications', 'amenities', 'license'
    ]
    
    # Aplicar limpeza apenas nas colunas existentes
    existing_text_cols = [col for col in text_cols if col in df.columns]
    for col in existing_text_cols:
        df[col] = df[col].apply(clean_text_field)
    
    # Converter tipos numéricos
    numeric_cols = [
        'price', 'accommodates', 'bathrooms', 'bedrooms', 'beds', 
        'review_scores_rating', 'number_of_reviews', 'minimum_nights',
        'maximum_nights', 'review_scores_accuracy', 'review_scores_cleanliness',
        'review_scores_checkin', 'review_scores_communication',
        'review_scores_location', 'review_scores_value'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            if col == 'price':
                df[col] = df[col].apply(clean_price)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Adicionar apenas processed_date (city e snapshot_date serão partições)
    df['processed_date'] = datetime.now().strftime('%Y-%m-%d')
    
    return df

def transform_calendar(df: pd.DataFrame, city: str, snapshot: str) -> pd.DataFrame:
    """Transformações para dados de calendar - mantém colunas originais"""
    df_processed = df.copy()
    
    # Limpeza básica
    df_processed = df_processed.replace(['', 'N/A', 'NaN', 'null', 'NULL'], np.nan)

    # Renomear coluna date para calendar_date
    if 'date' in df_processed.columns:
        df_processed = df_processed.rename(columns={'date': 'calendar_date'})
    
    # Limpar campos de texto
    text_cols = ['listing_id', 'available', 'date']
    for col in text_cols:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(clean_text_field)
    
    # Converter campos numéricos
    numeric_cols = ['price', 'adjusted_price', 'minimum_nights', 'maximum_nights']
    for col in numeric_cols:
        if col in df_processed.columns:
            if col in ['price', 'adjusted_price']:
                df_processed[col] = df_processed[col].apply(clean_price)
            else:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
    
    # Converter available para booleano consistente
    if 'available' in df_processed.columns:
        df_processed['available'] = df_processed['available'].astype(str).str.lower()
        df_processed['available'] = df_processed['available'].replace({'t': 'true', 'f': 'false'})
    
    # NÃO adicionar city e snapshot_date aqui (serão partições)
    return df_processed

def transform_reviews(df: pd.DataFrame, city: str, snapshot: str) -> pd.DataFrame:
    """Transformações para dados de reviews - tratamento especial para comments"""
    # Limpeza básica
    df = df.replace(['', 'N/A', 'NaN', 'null', 'NULL'], np.nan)
    
    # Renomear coluna date para review_date
    if 'date' in df.columns:
        df = df.rename(columns={'date': 'review_date'})
    
    # Limpar campos de texto (especialmente comments que podem ter vírgulas)
    text_cols = ['listing_id', 'id', 'reviewer_id', 'reviewer_name', 'comments']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_text_field)
    
    # Converter datas (agora review_date)
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    
    return df

def save_csv_with_proper_escaping(df: pd.DataFrame, file_path: str):
    """Salva DataFrame como CSV com escape adequado usando quoting=QUOTE_ALL"""
    try:
        # Usar quoting=csv.QUOTE_ALL para garantir que todos os campos sejam envolvidos em aspas
        df.to_csv(
            file_path, 
            index=False, 
            quoting=csv.QUOTE_ALL,  # Todas as colunas entre aspas
            escapechar='\\',        # Escape character para aspas dentro dos campos
            encoding='utf-8'
        )
        logger.info(f"CSV salvo com escape adequado: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar CSV com escape: {e}")
        # Tentar método alternativo se falhar
        try:
            df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"CSV salvo sem escape: {file_path}")
            return True
        except Exception as e2:
            logger.error(f"Falha completa ao salvar CSV: {e2}")
            return False

def process_file(file_path: str, data_type: str):
    """Processa um arquivo individual"""
    try:
        filename = os.path.basename(file_path)
        
        # Extrair cidade e snapshot do nome do arquivo
        parts = filename.split('_')
        city = parts[0]
        snapshot = parts[1]
        
        # Ler dados com tratamento robusto
        logger.info(f"Processando {data_type} para {city}")
        df = read_csv_safely(file_path)
        
        # Aplicar transformações baseadas no tipo
        if data_type == 'listings':
            df_processed = transform_listings(df, city, snapshot)
        elif data_type == 'calendar':
            df_processed = transform_calendar(df, city, snapshot)
        elif data_type == 'reviews':
            df_processed = transform_reviews(df, city, snapshot)
        else:
            logger.warning(f"Tipo de dados desconhecido: {data_type}")
            return None
        
        # Salvar arquivo processado com escape adequado
        output_filename = f"{city}_{snapshot}_{data_type}.csv"
        output_path = os.path.join(LOCAL_PROCESSED_DIR, data_type, output_filename)
        
        success = save_csv_with_proper_escaping(df_processed, output_path)
        
        if success:
            logger.info(f"Arquivo processado salvo: {output_path}")
            return output_path
        else:
            logger.error(f"Falha ao salvar arquivo processado: {output_path}")
            return None
        
    except Exception as e:
        logger.error(f"Erro processando arquivo {file_path}: {e}")
        return None

def main():
    ensure_directories()
    
    # Processar arquivos de cada subdiretório
    processed_files = []
    
    for data_type in ['listings', 'calendar', 'reviews']:
        type_dir = os.path.join(LOCAL_RAW_DIR, data_type)
        
        if os.path.exists(type_dir):
            for file in os.listdir(type_dir):
                if file.endswith('.csv'):
                    file_path = os.path.join(type_dir, file)
                    processed_path = process_file(file_path, data_type)
                    if processed_path:
                        processed_files.append(processed_path)
    
    logger.info(f"Transformação concluída. Arquivos processados: {len(processed_files)}")
    return processed_files

if __name__ == "__main__":
    main()