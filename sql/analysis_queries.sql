-- Requête 1 : Top 5 des marques avec la moyenne de sucre la plus élevée
-- Objectif : Identifier les marques proposant les produits les plus sucrés
SELECT 
    d.brand_name, 
    COUNT(*) as nb_products,
    ROUND(AVG(f.sugars_100g), 2) as avg_sugar
FROM off_datamart.fact_nutrition f
JOIN off_datamart.dim_product d ON f.product_code = d.code
WHERE d.brand_name != ''
GROUP BY d.brand_name
HAVING nb_products >= 2
ORDER BY avg_sugar DESC
LIMIT 5;

-- Requête 2 : Corrélation entre Nutriscore et Calories
-- Objectif : Vérifier la cohérence du datamart (Le grade E doit être plus calorique que le A)
SELECT 
    d.nutriscore_grade, 
    COUNT(*) as volume,
    ROUND(AVG(f.energy_kcal_100g), 0) as avg_calories
FROM off_datamart.dim_product d
JOIN off_datamart.fact_nutrition f ON f.product_code = d.code
WHERE d.nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
GROUP BY d.nutriscore_grade
ORDER BY d.nutriscore_grade;