# Documentación del Pipeline de Datos — Amaris Energía

## 1. Descripción del Pipeline

### Arquitectura General

El pipeline sigue un patrón **Medallion Architecture** con 4 capas de almacenamiento en S3, procesamiento orquestado por EventBridge y catalogación automática con Glue Crawlers.

```
[Sistema Transaccional]
         │
         ▼  (cada hora via EventBridge)
[Lambda Ingesta]  ──────────────────────────►  S3 Landing Zone
         │                                      (CSV originales)
         │  (evento: CSVIngestionCompleted)
         ▼
[EventBridge]  ──────────────────────────────►  [Glue ETL Job]
                                                      │
                    ┌─────────────────────────────────┤
                    ▼               ▼                 ▼
               S3 Raw           S3 Processed      Glue Catalog
             (CSV intacto)    (Parquet/Snappy)   (metadatos)
                    │               │
                    │               ▼
                    │          [Athena]  ◄── Python SDK
                    │               │
                    └───────────────┤
                                    ▼
                             S3 Curated  ──►  [Glue Redshift Job]
                                                      │
                                                      ▼
                                               Redshift DWH
```

---

## 2. Componentes del Pipeline

### 2.1 Ingesta Automática (Landing Zone)

**Servicio**: AWS Lambda + Amazon EventBridge

- **Trigger**: EventBridge cron `rate(1 hour)` — se ejecuta cada hora
- **Función**: `amaris-dev-csv-ingestion`
- **Lógica**: Lee CSVs del sistema transaccional y los deposita en S3 con particionamiento Hive por fecha de carga:
  ```
  s3://amaris-dev-landing/raw/{dataset}/year=YYYY/month=MM/day=DD/hour=HH/{archivo}.csv
  ```
- **Datasets**: `proveedores`, `clientes`, `transacciones`
- Al finalizar, emite un evento a EventBridge que dispara automáticamente el ETL

### 2.2 Zona Raw

**Servicio**: Amazon S3

- Preserva los CSVs **sin modificación** para trazabilidad y re-procesamiento
- Mismo esquema de particionamiento que Landing
- Lifecycle: Standard → Standard-IA (90 días) → Glacier (365 días)
- **Propósito**: audit trail, recuperación ante errores en ETL

### 2.3 ETL — 3 Transformaciones (Glue Job)

**Servicio**: AWS Glue 4.0 (Spark)  
**Job**: `amaris-dev-etl-energia-transform`

#### Transformación 1: Normalización y Limpieza
- Trim y lowercase en campos categóricos (tipo_energia, segmento)
- Cast de tipos: fechas a `DateType`, numéricos a `DoubleType`/`LongType`
- Filtrado de valores no permitidos (e.g. tipos de energía inválidos)
- Eliminación de duplicados por clave natural
- Enmascaramiento de identificaciones (últimos 4 dígitos)
- Campo `antiguedad_dias` calculado desde `fecha_vinculacion`

#### Transformación 2: Enriquecimiento de Transacciones
- `valor_total_cop = cantidad_kwh × precio_cop_kwh`
- Clasificación por volumen: pequeña (<1K kWh) / mediana (1K–100K) / grande (>100K)
- Dimensiones temporales: año, mes, día de semana
- Window functions: precio promedio por tipo de energía
- Flag `sobre_precio_promedio` y `desviacion_precio_pct`

#### Transformación 3: Integración Referencial y Métricas
- JOIN transacciones × clientes (enriquecer ventas con segmento/ciudad)
- JOIN transacciones × proveedores (enriquecer compras con capacidad_mw)
- Agregados por tipo_energia: total_kwh, total_cop, precios estadísticos
- Agregados por segmento: ticket_promedio, consumo_total
- Produce 5 tablas Parquet optimizadas

### 2.4 Zona Processed (Parquet)

**Formato**: Apache Parquet con compresión Snappy  
**Particionamiento**: `year=YYYY/month=MM/day=DD/`

Tablas:
| Tabla | Descripción |
|-------|-------------|
| `proveedores` | Proveedores normalizados |
| `clientes` | Clientes enriquecidos con antigüedad |
| `transacciones` | Hechos enriquecidos con métricas calculadas |
| `metricas_energia` | Agregados por tipo de energía |
| `metricas_segmento` | Agregados por segmento de cliente |

### 2.5 Catalogación Automática (Glue Crawlers)

**Crawlers configurados**:
| Crawler | Zona | Schedule |
|---------|------|----------|
| `amaris-dev-raw-crawler` | Raw | Cada hora `:00` |
| `amaris-dev-processed-crawler` | Processed | Cada hora `:30` |
| `amaris-dev-curated-crawler` | Curated | Diario 2am |

El Crawler **detecta automáticamente**:
- Esquemas de columnas y tipos de datos
- Nuevas particiones (`year/month/day`)
- Cambios en esquemas (agrega columnas, no elimina)
- Actualiza el Glue Data Catalog para que Athena pueda consultarlos

### 2.6 Consultas con Amazon Athena

**Workgroup**: `amaris-dev`  
**Database**: `amaris_datalake_dev_processed`

7 consultas SQL implementadas en `scripts/athena_queries.py`:
- Q1: Total de energía por tipo (compras)
- Q2: Top 5 clientes por consumo
- Q3: Precio promedio por tipo de energía y mercado
- Q4: Transacciones pendientes (alerta operacional)
- Q5: Margen estimado compra vs venta por tipo de energía
- Q6: Distribución de clientes por segmento y ciudad
- Q7: Proveedores activos con mayor capacidad instalada

### 2.7 Pipeline a Redshift (Plus)

**Glue Job**: `redshift_pipeline.py`  
**Target**: Redshift Serverless — `energia_dwh`

Modelo dimensional:
- `dim_proveedores` / `dim_clientes` — Dimensiones
- `fact_transacciones` — Tabla de hechos (DISTKEY: tipo_energia)
- `agg_metricas_energia` / `agg_metricas_segmento` — Agregados
- Vistas: `v_margen_energia`, `v_resumen_diario`

---

## 3. Permisos y Políticas IAM

### Principio Aplicado: Mínimo Privilegio

Cada servicio tiene su propio rol con acceso **exactamente** a los recursos que necesita.

### 3.1 Rol Lambda (`amaris-dev-lambda-role`)

```
Permite:
  ✅ s3:PutObject, GetObject, ListBucket → solo bucket Landing
  ✅ kms:GenerateDataKey, Decrypt → solo la KMS key del proyecto
  ✅ events:PutEvents → solo el event bus default
  ✅ glue:StartJobRun, GetJobRun → jobs del proyecto
  ✅ logs:CreateLogGroup/Stream/PutEvents → CloudWatch Logs (básico)

Deniega implícitamente:
  ❌ Acceso a Raw, Processed, Curated
  ❌ Modificar/eliminar datos existentes
  ❌ Acceso a otros proyectos/cuentas
```

**Proceso de configuración**:
1. En Terraform, el rol se crea con `aws_iam_role` + `aws_iam_role_policy`
2. La política usa ARNs específicos (no wildcards `*`) excepto CloudWatch Logs
3. Para producción: agregar `aws:RequestedRegion` condition para restringir región

### 3.2 Rol Glue (`amaris-dev-glue-role`)

```
Permite:
  ✅ AWSGlueServiceRole (managed policy — estándar de AWS)
  ✅ s3:GetObject, ListBucket → Landing, Raw, Scripts
  ✅ s3:PutObject, DeleteObject → Processed, Curated
  ✅ kms:Decrypt/GenerateDataKey → KMS del proyecto
  ✅ glue:* → solo databases/tables del proyecto (via Lake Formation)
```

**Proceso**:
1. Adjuntar `AWSGlueServiceRole` (policy administrada)
2. Agregar política inline para S3 específico del proyecto
3. En Lake Formation: registrar los buckets y asignar permisos de tabla

### 3.3 Rol Analyst (`amaris-dev-analyst-role`)

```
Permite (solo lectura):
  ✅ athena:StartQueryExecution, GetQueryResults → workgroup del proyecto
  ✅ s3:GetObject → Processed y Curated (no Landing ni Raw)
  ✅ glue:GetDatabase, GetTable, GetPartitions → catálogo
  ✅ kms:Decrypt → para leer datos cifrados

Lake Formation (column-level):
  ✅ Tabla clientes: solo columnas no sensibles + identificacion_enmascarada
  ❌ Columna identificacion (número real) — bloqueada por LF
```

**Proceso**:
1. Crear el rol con `assume_role_policy` que requiere `ExternalId` (evita confused deputy)
2. Configurar Lake Formation permissions para tablas específicas
3. Los analistas hacen `aws sts assume-role` con el ExternalId correcto

### 3.4 Configurar Lake Formation (pasos manuales adicionales)

```bash
# 1. Registrar administrador de Lake Formation
aws lakeformation put-data-lake-settings \
  --data-lake-settings '{"DataLakeAdmins":[{"DataLakePrincipalIdentifier":"arn:aws:iam::ACCOUNT:role/amaris-dev-glue-role"}]}'

# 2. Registrar buckets S3
aws lakeformation register-resource \
  --resource-arn arn:aws:s3:::amaris-dev-processed \
  --role-arn arn:aws:iam::ACCOUNT:role/amaris-dev-glue-role

# 3. Otorgar permisos de tabla a analista
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::ACCOUNT:role/amaris-dev-analyst-role \
  --permissions SELECT DESCRIBE \
  --resource '{"Table":{"DatabaseName":"amaris_datalake_dev_processed","Name":"transacciones"}}'
```

### 3.5 Cifrado y Seguridad

| Mecanismo | Configuración |
|-----------|---------------|
| Cifrado en reposo | KMS CMK con rotación automática anual |
| Cifrado en tránsito | TLS 1.2+ forzado en todos los endpoints |
| Acceso público | Bloqueado en todos los buckets S3 |
| Versionamiento | Habilitado — permite recuperar versiones anteriores |
| CloudTrail | Habilitar para auditoría completa de accesos |
| VPC | Para producción: usar VPC Endpoints para S3/Glue/Athena |

---

## 4. Instrucciones de Despliegue

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/amaris-datalake
cd amaris-datalake

# 1. Crear Lambda Layer y empaquetar código
cd terraform
make layer  # o zip -r lambda.zip ../lambda/

# 2. Inicializar y desplegar con Terraform
terraform init
terraform plan -var-file=environments/dev/terraform.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars

# 3. Subir datos de muestra
aws s3 cp data/sample/proveedores.csv   s3://amaris-dev-landing/raw/proveedores/year=2024/month=01/day=15/hour=00/
aws s3 cp data/sample/clientes.csv      s3://amaris-dev-landing/raw/clientes/year=2024/month=01/day=15/hour=00/
aws s3 cp data/sample/transacciones.csv s3://amaris-dev-landing/raw/transacciones/year=2024/month=01/day=15/hour=00/

# 4. Ejecutar ETL
aws glue start-job-run \
  --job-name amaris-dev-etl-energia-transform \
  --arguments '{"--PARTITION_DATE":"2024-01-15"}'

# 5. Ejecutar Crawlers
python glue_jobs/catalog_crawler_trigger.py --environment dev --zone all --wait

# 6. Consultar con Athena
python scripts/athena_queries.py --env dev --run all
```
