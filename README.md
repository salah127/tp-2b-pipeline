# TP 2B – Pipeline météo → transformation → PostgreSQL

## Objectif

Ce projet implémente un pipeline de données Airflow complet qui :

1. collecte des prévisions météo via l'API Open-Meteo pour plusieurs villes,
2. transforme les réponses JSON en un format tabulaire exploitable,
3. charge les données dans une base PostgreSQL,
4. enregistre un compte rendu d'exécution dans une table d'audit.

## Fichiers principaux

- `dags/weather_db_pipeline.py` : DAG principal (TP 2B) — pipeline avec persistance en base.
- `dags/weather_pipeline_dag.py` : DAG de référence (TP 2A) — export CSV simple.
- `sql/init_weather_tables.sql` : script SQL de création des tables PostgreSQL.
- `data/` : répertoire de travail pour les fichiers JSON et CSV intermédiaires.

## Architecture du pipeline

Le DAG `weather_db_pipeline` s'articule autour de cinq étapes :

1. `extract_weather` : récupération des données météo depuis l'API Open-Meteo,
2. `transform_weather` : mise à plat des réponses (une ligne par ville/date),
3. `create_db_tables` : création des tables PostgreSQL si elles n'existent pas,
4. `load_to_database` : insertion des données dans PostgreSQL,
5. `audit_pipeline_run` : enregistrement du bilan d'exécution dans la table d'audit.

```text
extract_weather --> transform_weather --> create_db_tables --> load_to_database --> audit_pipeline_run
```

## Données collectées

Champs extraits depuis l'API Open-Meteo :

| Champ | Description |
|---|---|
| `city` | Nom de la ville |
| `date` | Date de la prévision |
| `max_temperature_c` | Température maximale journalière (°C) |
| `min_temperature_c` | Température minimale journalière (°C) |
| `precipitation_mm` | Précipitations journalières (mm) |
| `timezone` | Fuseau horaire |

Villes traitées par défaut : **Strasbourg**, **Rennes**, **Nice**.

## Tables PostgreSQL

### `weather_data`
Stocke les mesures météo par ville et par date. En cas de doublon `(city, date)`, les valeurs sont mises à jour (`ON CONFLICT … DO UPDATE`).

### `pipeline_audit_log`
Conserve un enregistrement par exécution du DAG : nombre de villes traitées, nombre de lignes insérées, statut et message.

Le script de création est fourni dans `sql/init_weather_tables.sql`.

## Paramétrage du DAG

Le DAG `weather_db_pipeline` accepte les paramètres suivants via `dag_run.conf` :

| Paramètre | Valeur par défaut | Description |
|---|---|---|
| `postgres_conn_id` | `weather_db_conn` | Identifiant de la connexion Airflow |
| `weather_table` | `weather_data` | Table de données météo |
| `audit_table` | `pipeline_audit_log` | Table d'audit |
| `cities` | Strasbourg, Rennes, Nice | Liste des villes |
| `start_date` | date du jour | Date de début des prévisions |
| `end_date` | J+2 | Date de fin des prévisions |

## Instructions d'installation

### 1. Dépendances

- Airflow 3.x
- `apache-airflow-providers-postgres`
- `psycopg` ou `psycopg2`

```bash
pip install -r requirements.txt
```

### 2. Configuration Airflow

Créez une connexion Airflow PostgreSQL nommée `weather_db_conn` avec les informations suivantes :

- hôte,
- port,
- base de données,
- utilisateur,
- mot de passe.

### 3. Création des tables

Exécutez le script SQL sur la base PostgreSQL :

```bash
psql -h <host> -U <user> -d <database> -f sql/init_weather_tables.sql
```

### 4. Déploiement du DAG

Placez les fichiers du dossier `dags/` dans le répertoire `dags/` d'Airflow. Airflow détecte automatiquement les DAGs.

## Exécution

Démarrez Airflow puis utilisez l'interface web :

```bash
airflow standalone
```

Ouvrez ensuite :

```text
http://localhost:8080
```

Activez le DAG `weather_db_pipeline` et déclenchez une exécution manuelle.

## Vérification

Après exécution, vérifiez :

- la table `weather_data` contient les données météo,
- la table `pipeline_audit_log` contient une ligne de suivi,
- aucune erreur n'est apparue dans les logs des tâches.

## Exemple de structure cible

| city | date | max_temperature_c | min_temperature_c | precipitation_mm | timezone | ingestion_ts |
|------|------|-------------------|-------------------|------------------|----------|--------------|

Chaque ligne représente une prévision météo pour une ville à une date donnée.

## Suivi d'exécution

La table `pipeline_audit_log` contient le bilan de chaque run :

- `run_id`,
- `execution_date`,
- `city_count`,
- `row_count`,
- `status`,
- `message`.

Ce mécanisme garantit qu'une ligne de suivi est bien écrite pour chaque exécution.

## Remarques

- Le code sépare clairement récupération, transformation et chargement.
- Le DAG est paramétrable et extensible à de nouvelles villes ou tables.
- Le pipeline est conçu pour être facilement maintenable et compréhensible sans consulter le code.


## Architecture du pipeline

Le DAG s'articule autour de cinq étapes distinctes :

1. `extract_data` : appel de l'API Open-Meteo pour plusieurs villes,
2. `transform_data` : construction d'une table plate (une ligne par ville/date),
3. `ensure_postgres_tables` : création des tables cibles si nécessaire,
4. `load_data_to_postgres` : chargement effectif dans PostgreSQL,
5. `track_ingestion` : insertion d'une ligne de suivi dans la table d'audit.

Le flux d'exécution est le suivant :

```text
extract_data --> transform_data --> ensure_postgres_tables --> load_data_to_postgres --> track_ingestion
```

## Données retenues

Le pipeline extrait ces champs métiers depuis l'API Open-Meteo :

- `city` : nom de la ville,
- `date` : date de la prévision,
- `max_temperature_c` : température maximale journalière,
- `min_temperature_c` : température minimale journalière,
- `precipitation_mm` : quantité de précipitations journalières,
- `timezone` : fuseau horaire associé.

### Pourquoi ces champs ?

Ils constituent une base simple et cohérente pour une table analytique météo. Ils permettent de :

- comparer plusieurs villes,
- suivre l'évolution des températures,
- repérer les épisodes de pluie,
- charger des données dans un entrepôt ou un outil de BI.

## Tables PostgreSQL

Le projet utilise deux tables :

1. `weather_forecast`
   - stocke les mesures météo par ville et par date,
   - met à jour les données en cas de doublon (`ON CONFLICT`).

2. `weather_ingestion_audit`
   - conserve une ligne de suivi pour chaque exécution du DAG,
   - enregistre le nombre de villes traitées, le nombre de lignes insérées et le statut.

Le script de création est fourni dans `sql/create_postgres_tables.sql`.

## Paramétrage du DAG

Le DAG est paramétrable via :

- un `postgres_conn_id` Airflow (par défaut `open_meteo_postgres`),
- la liste des villes à traiter,
- les dates de début et de fin,
- les noms de tables PostgreSQL.

Ces paramètres peuvent être passés depuis `dag_run.conf` ou conservés par défaut.

## Instructions d'installation

### 1. Dépendances

Ce pipeline nécessite :

- Airflow 3.x,
- `apache-airflow-providers-postgres`,
- un connecteur PostgreSQL compatible (`psycopg` ou `psycopg2`).

Dans le virtualenv du projet, installez :

```bash
pip install -r requirements.txt
```

Le fichier `requirements.txt` contient les dépendances nécessaires au projet.

### 2. Configuration Airflow

Créez une connexion Airflow PostgreSQL nommée `open_meteo_postgres` avec les informations suivantes :

- hôte,
- port,
- base de données,
- utilisateur,
- mot de passe.

### 3. Création des tables

Exécutez le script SQL sur la base PostgreSQL :

```bash
psql -h <host> -U <user> -d <database> -f sql/create_postgres_tables.sql
```

### 4. Déploiement du DAG

Placez `dags/meteo_postgres_pipeline.py` dans le dossier `dags/`. Airflow doit détecter automatiquement le DAG.

## Exécution

Démarrez Airflow puis utilisez l'interface web :

```bash
airflow standalone
```

Ouvrez ensuite :

```text
http://localhost:8080
```

Activez le DAG `meteo_pipeline_postgres` et déclenchez une exécution manuelle.

## Vérification

Après exécution, vérifiez :

- la table `weather_forecast` contient les données météo,
- la table `weather_ingestion_audit` contient une ligne de suivi,
- aucune erreur n'est apparue dans les logs des tâches.

## Exemple de structure cible

| city | date | max_temperature_c | min_temperature_c | precipitation_mm | timezone | ingestion_ts |
|------|------|-------------------|-------------------|------------------|----------|--------------|

Chaque ligne représente une prévision météo pour une ville à une date donnée.

## Preuve de chargement

La table `weather_ingestion_audit` contient le suivi de chaque exécution :

- `run_id`,
- `execution_date`,
- `city_count`,
- `row_count`,
- `status`,
- `message`.

Ce mécanisme garantit qu'une ligne de suivi est bien écrite pour chaque run.

## Remarques

- Le code sépare clairement récupération, transformation et chargement.
- Le DAG est paramétrable et extensible à de nouvelles villes ou tables.
- Le pipeline est conçu pour être facilement maintenable et compréhensible sans consulter le code.
