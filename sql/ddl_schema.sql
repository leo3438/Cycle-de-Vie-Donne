CREATE DATABASE IF NOT EXISTS off_datamart;
USE off_datamart;

-- Nettoyage préalable
DROP TABLE IF EXISTS fact_nutrition;
DROP TABLE IF EXISTS dim_product;

-- 1. Table Dimension Produit (SCD2 Ready)
CREATE TABLE dim_product (
    product_sk INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(255) NOT NULL,
    product_name VARCHAR(500),
    brand_name VARCHAR(255),
    nutriscore_grade VARCHAR(50),
    
    -- Champs techniques pour l'historisation
    effective_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    effective_to DATETIME DEFAULT '2099-12-31 00:00:00',
    is_current BOOLEAN DEFAULT 1,
    
    INDEX idx_code (code)
);

-- 2. Table de Faits Nutrition
CREATE TABLE fact_nutrition (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    product_code VARCHAR(255),
    
    -- Mesures
    energy_kcal_100g FLOAT,
    sugars_100g FLOAT,
    salt_100g FLOAT,
    proteins_100g FLOAT,
    
    imported_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_fact_code (product_code)
);