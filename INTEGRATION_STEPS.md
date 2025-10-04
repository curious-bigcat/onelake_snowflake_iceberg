## Snowflake ↔ Microsoft Fabric (OneLake) Iceberg Integration — Quick Steps

Follow these steps to set up an Iceberg table in Snowflake backed by OneLake storage in Microsoft Fabric, load sample data, and access it from Fabric via a shortcut.

### Prerequisites
- **Snowflake**: Ability to use the `ACCOUNTADMIN` role (or equivalent privileges).
- **Microsoft Fabric**: Admin Portal access to enable service principals, and Contributor permissions on the target Workspace.
- **Fabric Lakehouse**: Create a Lakehouse (e.g., `<LakehouseName>` like `snowflakeQS`) in your target Workspace.
- **Collect**:
  - **OneLake Base URL**: In Fabric Lakehouse → Explorer → Files → ellipsis → Properties → copy URL, then replace `https://` with `azure://`.
  - **AZURE_TENANT_ID**: Fabric UI → Help (?) → About Fabric → copy the UUID after `ctid=`.

---

### 1) Snowflake: Warehouse, Database, Schema, and Sample Data
Copy into a Snowflake SQL Worksheet and run.

```sql
-- Use admin role
USE ROLE ACCOUNTADMIN;

-- Create a small warehouse
CREATE OR REPLACE WAREHOUSE HOL_WH WITH
  WAREHOUSE_SIZE = 'X-SMALL'
, AUTO_SUSPEND = 60
, AUTO_RESUME = TRUE
, INITIALLY_SUSPENDED = TRUE;

-- Create DB and schema for this integration
CREATE OR REPLACE DATABASE SnowflakeQS;
USE DATABASE SnowflakeQS;

CREATE OR REPLACE SCHEMA IcebergTest;
USE SCHEMA IcebergTest;

-- Create the sample data database from public share
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;

-- Grant public role read access to the sample data
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE PUBLIC;

-- Quick sanity check
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.customer LIMIT 5;
```

---

### 2) Snowflake: Create External Volume pointing to OneLake
Replace placeholders before running.

```sql
-- Replace the STORAGE_BASE_URL and AZURE_TENANT_ID
-- STORAGE_BASE_URL format example:
--   azure://onelake.dfs.fabric.microsoft.com/<FabricWorkspaceName>/<LakehouseName>.Lakehouse/Files/

CREATE OR REPLACE EXTERNAL VOLUME FabricExVol
  STORAGE_LOCATIONS = (
    (
      NAME = 'FabricExVol'
      , STORAGE_PROVIDER = 'AZURE'
      , STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<FabricWorkspaceName>/<LakehouseName>.Lakehouse/Files/'
      , AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
    )
  );
```

Tips:
- Ensure the URL scheme is `azure://` (not `https://`).
- The path should point to the Lakehouse `Files/` container root or a desired subfolder.

---

### 3) Enable Permissions for Snowflake to Access Fabric
1. In Snowflake, get the service principal name:
   ```sql
   DESC EXTERNAL VOLUME FabricExVol;
   ```
   - In the output JSON under `property_value` for the storage location, find `AZURE_MULTI_TENANT_APP_NAME`.
   - Copy this value (you may remove the trailing underscore and numbers when searching in Fabric).

2. In Fabric Admin Portal:
   - Go to Admin portal → Developer settings → enable "Service principals can use Fabric APIs".
   - Under OneLake settings → enable "Users can access data stored in OneLake with apps external to Fabric".

3. Grant the service principal access to your Workspace:
   - Open the Fabric Workspace → Manage access → Add people or groups.
   - Search for the service principal from step 1.
   - Add as **Contributor**.

---

### 4) Snowflake: Create Iceberg Table in OneLake and Load Sample Data
```sql
-- Create the Iceberg table for spotify.csv backed by the external volume
CREATE OR REPLACE ICEBERG TABLE SnowflakeQS.ICEBERGTEST.spotify_users (
  user_id INT,
  gender STRING,
  age INT,
  country STRING,
  subscription_type STRING,
  listening_time INT,
  songs_played_per_day INT,
  skip_rate DOUBLE
)
EXTERNAL_VOLUME = 'FabricExVol'
CATALOG = snowflake
BASE_LOCATION = 'spotify_users';

-- Load your CSV via COPY INTO from a stage (see step 3 and the load section)

-- Quick query in Snowflake
SELECT * FROM SnowflakeQS.ICEBERGTEST.spotify_users LIMIT 10;
```

Optional: get the physical storage path for the table.
```sql
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('SnowflakeQS.ICEBERGTEST.SPOTIFY_USERS');
```

You should see a JSON response with a `metadataLocation` like:
`azure://<storage_account_path>/<path-within-storage>/spotify_users/metadata/<file>.metadata.json`

---

### 5) Fabric: Create a OneLake Shortcut to the Iceberg Table
1. Workspace settings → OneLake → enable "Enable Delta Lake to Apache Iceberg table format virtualization" (Preview).
2. Open your Fabric Lakehouse (e.g., `<LakehouseName>` like `snowflakeQS`).
3. Click "New shortcut" from Tables.
4. Select a **OneLake Shortcut**.
5. Enter the connection using the same OneLake base path you used in Snowflake.
6. Navigate to the table folder (e.g., `spotify_users/`) and select the folder itself.
   - Do NOT select `data/` or `metadata/` subfolders.
7. Click Next → Create to finish.

Notes:1
- If your Lakehouse is schema-enabled, create the shortcut under the schema (for example, `dbo`). If not schema-enabled, create it directly under `Tables`.
- OneLake shortcuts for this feature must be in the same region as the target location.

You should now see the Iceberg table available in the Lakehouse Tables. Use the Lakehouse SQL endpoint or notebooks to query.

---

### 6) Validation
- In Snowflake:
  - `SELECT COUNT(*) FROM SnowflakeQS.ICEBERGTEST.spotify_users;`
  - `SELECT * FROM SnowflakeQS.ICEBERGTEST.spotify_users LIMIT 10;`
- In Fabric:
  - Refresh the Lakehouse → confirm the table appears under Tables.
  - Query via the SQL endpoint or open a Notebook and read from the table.

---

### Troubleshooting
- If `DESC EXTERNAL VOLUME` does not show `AZURE_MULTI_TENANT_APP_NAME`, verify `STORAGE_PROVIDER = 'AZURE'` and the `azure://` URL.
- Permission errors when creating/loading the table usually indicate the service principal lacks Workspace access or the Admin toggle is off.
- Verify the `AZURE_TENANT_ID` matches the value from Fabric "About" (the UUID after `ctid=`).
- Ensure the Lakehouse path is correct and points to `.../<LakehouseName>.Lakehouse/Files/`.

- If the table does not appear as a usable Delta table in Fabric, ensure the shortcut targets the table folder and is created under `Tables` (or under a schema if schema-enabled), not under `Files`.
- If you drop and recreate an Iceberg table in the same folder from Snowflake, remove old metadata so only one set of Iceberg metadata files remains; multiple sets prevent conversion.
- If preview errors mention Parquet type conversions for INT64/double/decimal, try the Lakehouse SQL Endpoint view. In Spark notebooks, set `spark.sql.parquet.enableVectorizedReader=false`.
- Check `_delta_log/latest_conversion_log.txt` under the shortcut for the latest conversion success/failure details.
- Certain Iceberg partition transforms (`bucket[N]`, `truncate[W]`, `void`) are not supported by the virtualization.
- Private links are not supported.
- Do not move/copy Iceberg table folders without rewriting metadata; absolute references in Iceberg metadata are not portable.
- The feature has region limitations; ensure the shortcut and target are in the same region and that your region is supported in Fabric.

---

### 7) Ingest Local CSVs (movies, ratings) into Fabric as Delta Tables
Use your existing Lakehouse to create Delta tables from CSVs and enable virtualization to Iceberg for Snowflake reads.

Steps (Fabric UI):
1. Open your Lakehouse (e.g., `<LakehouseName>` like `snowflakeQS`).
2. Ensure the virtualization toggle is ON per step 5.
3. Place CSVs into the Lakehouse `Files/` area (e.g., upload `Files/movies/movies.csv` and `Files/ratings/ratings.csv`).
4. In Lakehouse → Data Engineering experience:
   - Create table from file for `movies.csv`:
     - Source: `Files/movies/movies.csv`
     - Destination: Tables → choose schema (e.g., `dbo`) → table name `movies`
     - Format: CSV → infer schema → set delimiter and header = true
     - Output format: Delta
   - Repeat for `ratings.csv` to create table `ratings` in the same schema.
5. Validate in Fabric:
   - Confirm both `movies` and `ratings` appear under Tables (or under `dbo` if schema‑enabled).
   - Preview data or query via SQL endpoint, e.g. `SELECT TOP 10 * FROM dbo.movies`.

Optional (Notebook example – PySpark):
```python
# Replace <LakehouseName> and paths as appropriate
movies_df = spark.read.option("header", True).csv("/lakehouse/default/Files/movies/movies.csv")
ratings_df = spark.read.option("header", True).csv("/lakehouse/default/Files/ratings/ratings.csv")

movies_df.write.mode("overwrite").format("delta").saveAsTable("dbo.movies")
ratings_df.write.mode("overwrite").format("delta").saveAsTable("dbo.ratings")
```

---

### 8) Read These Tables from Snowflake via Virtual Iceberg
Once the Delta tables exist in Fabric, OneLake virtualizes them as Iceberg. Reference them from Snowflake.

1. Ensure Admin settings are enabled (Step 3 and Step 5).
2. Create a read‑only External Volume to the Lakehouse `Tables/` path (schema‑enabled example):
```sql
CREATE OR REPLACE EXTERNAL VOLUME onelake_read_exvol
STORAGE_LOCATIONS =
(
    (
        NAME = 'onelake_read_exvol'
        , STORAGE_PROVIDER = 'AZURE'
        , STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<WorkspaceId>/<LakehouseId>/Tables/'
        , AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
    )
)
ALLOW_WRITES = false;
```

3. Retrieve consent URL and app name, then grant access in Fabric Workspace if not already done:
```sql
DESC EXTERNAL VOLUME onelake_read_exvol;
```

4. Create the Catalog Integration (if not already present):
```sql
CREATE CATALOG INTEGRATION onelake_catalog_integration
CATALOG_SOURCE = OBJECT_STORE
TABLE_FORMAT = ICEBERG
ENABLED = TRUE;
```

5. Find the latest metadata file path for each table in Fabric:
- Path pattern (schema‑enabled): `dbo/<TableName>/metadata/<latest>.metadata.json`.
- Example: `dbo/movies/metadata/321.metadata.json` and `dbo/ratings/metadata/321.metadata.json`.

6. Create Snowflake Iceberg table references for each:
```sql
CREATE OR REPLACE ICEBERG TABLE SnowflakeQS.ICEBERGTEST.movies
EXTERNAL_VOLUME = 'onelake_read_exvol'
CATALOG = onelake_catalog_integration
METADATA_FILE_PATH = 'dbo/movies/metadata/<latest>.metadata.json';

CREATE OR REPLACE ICEBERG TABLE SnowflakeQS.ICEBERGTEST.ratings
EXTERNAL_VOLUME = 'onelake_read_exvol'
CATALOG = onelake_catalog_integration
METADATA_FILE_PATH = 'dbo/ratings/metadata/<latest>.metadata.json';
```

7. Validate in Snowflake:
```sql
SELECT COUNT(*) FROM SnowflakeQS.ICEBERGTEST.movies;
SELECT COUNT(*) FROM SnowflakeQS.ICEBERGTEST.ratings;
SELECT * FROM SnowflakeQS.ICEBERGTEST.movies LIMIT 10;
SELECT * FROM SnowflakeQS.ICEBERGTEST.ratings LIMIT 10;
```


---
