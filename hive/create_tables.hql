USE inside_airbnb;

-- Tabela de listings (mantida como antes)
CREATE EXTERNAL TABLE IF NOT EXISTS listings (
    id STRING,
    listing_url STRING,
    scrape_id STRING,
    last_scraped STRING,
    source STRING,
    name STRING,
    description STRING,
    neighborhood_overview STRING,
    picture_url STRING,
    host_id STRING,
    host_url STRING,
    host_name STRING,
    host_since STRING,
    host_location STRING,
    host_about STRING,
    host_response_time STRING,
    host_response_rate STRING,
    host_acceptance_rate STRING,
    host_is_superhost STRING,
    host_thumbnail_url STRING,
    host_picture_url STRING,
    host_neighbourhood STRING,
    host_listings_count STRING,
    host_total_listings_count STRING,
    host_verifications STRING,
    host_has_profile_pic STRING,
    host_identity_verified STRING,
    neighbourhood STRING,
    neighbourhood_cleansed STRING,
    neighbourhood_group_cleansed STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    property_type STRING,
    room_type STRING,
    accommodates INT,
    bathrooms DOUBLE,
    bathrooms_text STRING,
    bedrooms DOUBLE,
    beds DOUBLE,
    amenities STRING,
    price DOUBLE,
    minimum_nights INT,
    maximum_nights INT,
    minimum_minimum_nights INT,
    maximum_minimum_nights INT,
    minimum_maximum_nights INT,
    maximum_maximum_nights INT,
    minimum_nights_avg_ntm DOUBLE,
    maximum_nights_avg_ntm DOUBLE,
    calendar_updated STRING,
    has_availability STRING,
    availability_30 INT,
    availability_60 INT,
    availability_90 INT,
    availability_365 INT,
    calendar_last_scraped STRING,
    number_of_reviews INT,
    number_of_reviews_ltm INT,
    number_of_reviews_l30d INT,
    availability_eoy INT,
    number_of_reviews_ly INT,
    estimated_occupancy_l365d DOUBLE,
    estimated_revenue_l365d DOUBLE,
    first_review STRING,
    last_review STRING,
    review_scores_rating DOUBLE,
    review_scores_accuracy DOUBLE,
    review_scores_cleanliness DOUBLE,
    review_scores_checkin DOUBLE,
    review_scores_communication DOUBLE,
    review_scores_location DOUBLE,
    review_scores_value DOUBLE,
    license STRING,
    instant_bookable STRING,
    calculated_host_listings_count INT,
    calculated_host_listings_count_entire_homes INT,
    calculated_host_listings_count_private_rooms INT,
    calculated_host_listings_count_shared_rooms INT,
    reviews_per_month DOUBLE,
    processed_date STRING
)
PARTITIONED BY (city STRING, snapshot_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar" = "\"",
   "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION 'gs://airbnb-data-pedro/inside_airbnb/listings/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Tabela de calendar 
CREATE EXTERNAL TABLE IF NOT EXISTS calendar (
    listing_id STRING,
    calendar_date STRING,
    available STRING,
    price DOUBLE,
    adjusted_price DOUBLE,
    minimum_nights INT,
    maximum_nights INT
)
PARTITIONED BY (city STRING, snapshot_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar" = "\"",
   "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION 'gs://airbnb-data-pedro/inside_airbnb/calendar/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Tabela de reviews 
CREATE EXTERNAL TABLE IF NOT EXISTS reviews (
    listing_id STRING,
    id STRING,
    review_date STRING,
    reviewer_id STRING,
    reviewer_name STRING,
    comments STRING
)
PARTITIONED BY (city STRING, snapshot_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar" = "\"",
   "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION 'gs://airbnb-data-pedro/inside_airbnb/reviews/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Recarregar partições
MSCK REPAIR TABLE listings;
MSCK REPAIR TABLE calendar;
MSCK REPAIR TABLE reviews;