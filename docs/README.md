# Projet Big Data - Intégration OpenFoodFacts

## Description
Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour analyser les données nutritionnelles d'OpenFoodFacts. Il utilise Apache Spark pour le traitement distribué et MySQL pour le stockage analytique (Datamart).

## Architecture Technique
* **Infrastructure :** Conteneur LXC (Debian 13) sur Proxmox.
* **ETL Engine :** Apache Spark 3.5 (PySpark).
* **Database :** MySQL 8.0 Community Server.
* **Langage :** Python 3.11 / Java 17 (Runtime).

## Structure du Projet
* `/docs` : Documentation et notes d'architecture.
* `/etl` : Scripts de traitement de données (PySpark).
* `/sql` : Scripts de création de base (DDL) et requêtes d'analyse.
* `/logs` : Rapports d'exécution automatiques (JSON).

## Installation & Lancement

### 1. Pré-requis
* Avoir Docker ou un environnement Linux avec Java 17 et Spark 3.5.
* Serveur MySQL 8 accessible.

### 2. Initialisation de la Base de Données
Exécuter le script SQL pour créer le schéma en étoile :
```bash
mysql -u root -p < sql/ddl_schema.sql