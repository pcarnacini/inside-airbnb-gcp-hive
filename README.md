# Inside Airbnb – Projeto de Análise de Dados com Hive e GCP

## 1. Tema do Projeto
Este projeto tem como foco a análise dos dados do **Inside Airbnb**, explorando preços, disponibilidade, comportamento de anfitriões e hóspedes, além da dinâmica de avaliações.  

O objetivo é aplicar um pipeline completo de **ETL + ingestão em Hive + consultas SQL analíticas**, demonstrando boas práticas em engenharia e análise de dados.

---

## 2. Fontes de Dados
- **Inside Airbnb (dados abertos):**
  - `listings.csv` → informações de anúncios e anfitriões  
  - `calendar.csv` → disponibilidade diária e preços  
  - `reviews.csv` → avaliações dos hóspedes  

- **Configuração:**  
  Arquivo `config/cities.yaml` define cidades, snapshot e URLs de origem.

---

## 3. Esquema de Dados (Hive)
O banco foi construído no Hive, com partições por **cidade** e **snapshot_date**.

### 3.1 Tabelas principais
1. **listings**  
   - `id`, `name`, `host_id`, `host_name`, `room_type`, `property_type`,  
     `neighbourhood_cleansed`, `price`, `accommodates`,  
     `number_of_reviews`, `review_scores_rating`.  

2. **calendar**  
   - `listing_id`, `calendar_date`, `available`, `price`, `adjusted_price`.  

3. **reviews**  
   - `id`, `listing_id`, `reviewer_id`, `reviewer_name`,  
     `review_date`, `comments`.  

---

## 4. Pipeline ETL
O pipeline foi desenvolvido em **Python, Shell e Hive**, e é executado pelo script `run_pipeline.sh`.

1. **Extract** – `extract_data.py` baixa arquivos e extrai `.gz`.  
2. **Transform** – `transform_data.py` aplica limpeza, padronização de preços e tratamento de texto.  
3. **Load** – `load_gcp.py` envia arquivos tratados para Google Cloud Storage.  
4. **Ingest** – `ingest_to_hive.sh` cria database e tabelas Hive.  
5. **Análises** – consultas SQL em `analyses.sql`.  

---

## 5. Perguntas Analíticas
O projeto responde a questões como:  

1. **Média de preço por tipo de quarto e bairro**
→ Quais tipos de quarto são mais caros em cada bairro e como os preços variam entre regiões?

2. **Disponibilidade diária total e percentual**
→ Qual é a taxa de disponibilidade diária dos anúncios e como ela evolui ao longo do tempo?

3. **Top 10 listings com mais reviews e média de preço**
→ Quais anúncios concentram o maior número de avaliações e qual é seu preço médio?

4. **Top 5 reviewers mais ativos**
→ Quem são os hóspedes que mais deixam avaliações e qual a sua recorrência de uso da plataforma?

5. **Distribuição de tipos de propriedade por bairro**
→ Quais são os tipos de propriedades mais comuns em cada bairro e como se distribuem geograficamente?

6. **Preço médio por número de acomodações e tipo de quarto**
→ Como o preço médio varia em função da capacidade de hóspedes e do tipo de quarto oferecido?

7. **Listings com poucas reviews**
→ Quais anúncios têm poucas avaliações e como isso se relaciona com seu preço médio?

8. **Estatísticas por host**
→ Quais anfitriões possuem mais de um anúncio e como variam seus preços médios e avaliações?

9. **Estatísticas básicas gerais**
→ Qual é o panorama geral do marketplace (número de anúncios, preços mínimos/máximos, diversidade de bairros, tipos de quarto e hosts)?

10. **Top bairros por preço médio**
→ Quais bairros apresentam os preços médios mais altos e como se compara a variação entre eles?

11. **Análise de reviews por mês**
→ Qual a evolução temporal do volume de reviews e quais tendências de sazonalidade podem ser observadas?

---

## 6. Consultas SQL

Todas as queries estão em `analyses.sql`.  

### 1. Média de preço por tipo de quarto e bairro
```sql
SELECT 
    l.room_type, 
    l.neighbourhood_cleansed, 
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price, 
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND CAST(c.price AS DOUBLE) > 0
  AND l.room_type IS NOT NULL 
  AND l.neighbourhood_cleansed IS NOT NULL
GROUP BY l.room_type, l.neighbourhood_cleansed
ORDER BY avg_price DESC
LIMIT 50;
```

### 2. Disponibilidade diária total e percentual
```sql
SELECT 
    calendar_date, 
    COUNT(*) AS total_listings,
    SUM(CASE WHEN available = 'true' THEN 1 ELSE 0 END) AS available_listings,
    ROUND(SUM(CASE WHEN available = 'true' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_available
FROM calendar
WHERE calendar_date IS NOT NULL
GROUP BY calendar_date
ORDER BY calendar_date
LIMIT 30;
```

### 3. Top 10 listings com mais reviews e média de preço
```sql
SELECT 
    l.id, 
    l.name, 
    COUNT(r.id) AS num_reviews,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price
FROM listings l
LEFT JOIN reviews r ON l.id = r.listing_id
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL
GROUP BY l.id, l.name
HAVING COUNT(r.id) > 0
ORDER BY num_reviews DESC
LIMIT 10;
```

### 4. Top 5 reviewers mais ativos
```sql
SELECT 
    reviewer_id, 
    reviewer_name, 
    COUNT(*) AS num_reviews,
    MIN(review_date) AS first_review, 
    MAX(review_date) AS last_review
FROM reviews
WHERE reviewer_id IS NOT NULL 
  AND review_date IS NOT NULL
GROUP BY reviewer_id, reviewer_name
ORDER BY num_reviews DESC
LIMIT 5;
```

### 5. Distribuição de tipos de propriedade por bairro
```sql
SELECT 
    l.neighbourhood_cleansed,
    l.property_type,
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
WHERE l.neighbourhood_cleansed IS NOT NULL 
  AND l.neighbourhood_cleansed != ''
  AND l.property_type IS NOT NULL 
  AND l.property_type != ''
GROUP BY l.neighbourhood_cleansed, l.property_type
ORDER BY total_listings DESC
LIMIT 50;
```

### 6. Preço médio por número de acomodações e tipo de quarto
```sql
SELECT 
    CAST(l.accommodates AS INT) AS accommodates_num, 
    l.room_type, 
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    COUNT(DISTINCT l.id) AS total_listings
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
  AND l.accommodates IS NOT NULL
  AND l.accommodates != ''
  AND l.room_type IS NOT NULL
  AND l.room_type != ''
GROUP BY CAST(l.accommodates AS INT), l.room_type
ORDER BY avg_price DESC;
```

### 7. Listings com poucas reviews
```sql
SELECT 
    l.id, 
    l.name, 
    CAST(l.number_of_reviews AS INT) AS total_reviews,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.number_of_reviews IS NOT NULL 
  AND l.number_of_reviews != ''
  AND l.name IS NOT NULL 
  AND l.name != ''
  AND c.price IS NOT NULL
  AND c.price != ''
GROUP BY l.id, l.name, l.number_of_reviews
ORDER BY total_reviews ASC
LIMIT 20;
```

### 8. Estatísticas por host
```sql
SELECT 
    l.host_id,
    l.host_name,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price_per_host,
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(l.review_scores_rating AS DOUBLE)), 2) AS avg_rating
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.host_id IS NOT NULL 
  AND l.host_id != ''
  AND c.price IS NOT NULL
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
GROUP BY l.host_id, l.host_name
HAVING COUNT(DISTINCT l.id) > 1
ORDER BY avg_rating DESC
LIMIT 10;
```

### 9. Estatísticas básicas gerais
```sql
SELECT
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    ROUND(MIN(CAST(c.price AS DOUBLE)), 2) AS min_price,
    ROUND(MAX(CAST(c.price AS DOUBLE)), 2) AS max_price,
    COUNT(DISTINCT l.room_type) AS unique_room_types,
    COUNT(DISTINCT l.neighbourhood_cleansed) AS unique_neighbourhoods,
    COUNT(DISTINCT l.host_id) AS unique_hosts
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE c.price IS NOT NULL 
  AND c.price != '';
```

### 10. Top bairros por preço médio
```sql
SELECT 
    l.neighbourhood_cleansed,
    COUNT(DISTINCT l.id) AS total_listings,
    ROUND(AVG(CAST(c.price AS DOUBLE)), 2) AS avg_price,
    ROUND(MIN(CAST(c.price AS DOUBLE)), 2) AS min_price,
    ROUND(MAX(CAST(c.price AS DOUBLE)), 2) AS max_price
FROM listings l
JOIN calendar c ON l.id = c.listing_id
WHERE l.neighbourhood_cleansed IS NOT NULL 
  AND l.neighbourhood_cleansed != ''
  AND c.price IS NOT NULL 
  AND c.price != ''
  AND CAST(c.price AS DOUBLE) > 0
GROUP BY l.neighbourhood_cleansed
HAVING COUNT(DISTINCT l.id) >= 5
ORDER BY avg_price DESC
LIMIT 20;
```

### 11. Análise de reviews por mês
```sql
SELECT 
    SUBSTR(review_date, 1, 7) AS review_month,
    COUNT(*) AS total_reviews
FROM reviews
WHERE review_date IS NOT NULL 
  AND review_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
GROUP BY SUBSTR(review_date, 1, 7)
ORDER BY review_month DESC
LIMIT 24;
```

---

## 7. Resultados

1. **Preços por bairro e tipo de quarto**

Encontramos valores fora da curva (ex.: quarto compartilhado em São Conrado a R$ 25.000 e imóveis em Estácio a R$ 500.000).

Isso evidencia a necessidade de tratamento de outliers nos dados de preço.

Considerando bairros consolidados como Copacabana, Ipanema e Leblon, os preços médios se mostraram consistentes com a realidade de mercado:

Copacabana: ~R$ 655 (mais de 12.000 anúncios)

Ipanema: ~R$ 956 (3.600 anúncios)

Leblon: ~R$ 1.010 (1.800 anúncios)

2. **Disponibilidade diária**

Observamos crescimento da disponibilidade ao longo do tempo, de 17% em 19/03/2025 para cerca de 68% em abril/2025.

Isso sugere baixa ocupação no período analisado ou forte concentração de anúncios ativos em datas futuras.

3. **Anúncios com mais reviews**

Alguns anúncios em Copacabana e Ipanema concentram centenas de milhares de avaliações, o que é estatisticamente improvável.

Provavelmente trata-se de dados duplicados ou inconsistentes no dataset.

Mesmo assim, conseguimos identificar que os imóveis mais avaliados têm preços médios acessíveis (R$ 60–300), indicando que imóveis de preço médio tendem a atrair mais reservas.

4. **Reviewers mais ativos**

Usuários como Rafael (67 avaliações desde 2022) demonstram uso recorrente da plataforma.

Esse tipo de análise ajuda a identificar perfis de hóspedes fiéis.


5. **Distribuição de tipos de propriedade**

O mercado é dominado por “Entire rental unit” (apartamentos completos):

Copacabana: 10.149 anúncios

Ipanema: 2.862 anúncios

Barra da Tijuca: 2.289 anúncios

Isso mostra que a oferta é concentrada em apartamentos inteiros, mais do que quartos privados ou compartilhados.

6. **Preço médio por capacidade**

Há crescimento do preço médio conforme o número de hóspedes, mas de forma não linear:

Acomodações para 4 pessoas → ~R$ 692

Acomodações para 8 pessoas → ~R$ 1.809

Acomodações para 16 pessoas → ~R$ 4.751

Notamos que quartos compartilhados chegam a valores altos (R$ 1.642 para 8 pessoas), o que indica possíveis distorções no cadastro.

7. **Anúncios com poucas reviews**

Grande volume de anúncios sem avaliações ou com reviews muito baixos.

Muitos deles têm preços desproporcionais (ex.: R$ 10.000 por noite em Ipanema e Leblon).

Essa análise reforça que novos anúncios ainda não validados pelo mercado são outliers importantes.

8. **Hosts com múltiplos anúncios**

Identificamos anfitriões com mais de 2 imóveis listados e avaliações médias muito altas (5.0).

Esses hosts podem representar operadores profissionais de aluguel de temporada.

9. **Estatísticas gerais**

Copacabana, Ipanema e Barra concentram a maior parte da oferta.

Preços mínimos são muito baixos (R$ 15–20), enquanto máximos chegam a R$ 500.000 → forte evidência de dados espúrios.

10. **Top bairros por preço médio**

Bairros com preços mais altos (Joá, Estácio, Itanhangá) sofrem influência de outliers.

Quando filtramos valores plausíveis, bairros tradicionais como Leblon, Ipanema e Lagoa permanecem entre os mais caros.

11. **Tendência de reviews por mês**

Forte sazonalidade:

Janeiro/2025 → 47.135 reviews (alta temporada)

Dezembro/2023 → 15.129 reviews (baixa)

Mostra picos de procura no verão e em meses de férias.

---

## 8. Próximos Passos
- Expandir para mais cidades e snapshots históricos.  
- Cruzar com dados externos (IBGE, turismo, eventos).  
- Aplicar modelos de previsão de preço/ocupação. 
- Migrar a transformação de dados (atualmente em Pandas) para PySpark. Isso permitirá escalar para volumes de dados muito maiores, aproveitando clusters do Dataproc/GCP.
- Reescrever consultas analíticas em Spark SQL, possibilitando execução mais rápida sobre datasets extensos e integração nativa com o Hive Metastore.
- Salvar dados processados em Parquet/ORC em vez de CSV, reduzindo armazenamento, acelerando queries e permitindo compressão nativa no Spark.
- Usar particionamento dinâmico em Spark (por city e snapshot_date), melhorando performance de leitura e consultas analíticas.
- Construir modelos de previsão de preço e demanda utilizando o pipeline de MLlib, treinando em larga escala.
- Conectar dados processados pelo Spark diretamente a ferramentas como Databricks SQL ou Power BI para análises em tempo quase real.
- Alterar o run_pipeline.sh para submissão de jobs Spark (spark-submit) e agendar no Airflow, trazendo robustez e escalabilidade ao pipeline.

---

## 9. Entregáveis
- Relatório/documentação (`README.md`).  
- Scripts Hive (`create_database.hql`, `create_tables.hql`).  
- Scripts ETL (`extract_data.py`, `transform_data.py`, `load_gcp.py`).  
- Shell scripts (`run_pipeline.sh`, `ingest_to_hive.sh`).  
- Consultas (`analyses.sql`).

---

## 10. Como replicar?

### Instalar dependências
pip install -r requirements.txt

### Configurar Google Cloud SDK
gcloud auth login
gcloud config set project planar-alliance-467623-f1 -- Altera pro ID do teu projeto assim como no load_gcp.py

### Configurar variáveis de ambiente
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"

### Executar pipeline completo
```bash
run_pipeline.sh
```

### Ou executar etapa por etapa
```python
python extract_data.py
python transform_data.py
python load_gcp.py
bash ingest_to_hive.sh
```

### Executar consultas SQL no Hive
```bash
gcloud dataproc jobs submit hive \
  --cluster airbnb-hive-clusterx \
  --region us-central1 \
  --file analyses.sql
```
