-- scripts/redshift_ddl.sql
-- DDL para el esquema del Data Warehouse en Redshift
-- Modelo dimensional: dimensiones + hechos

-- ── Crear esquema ────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS energia_dwh;

-- ── Dimensión Proveedores ────────────────────────────────────
CREATE TABLE IF NOT EXISTS energia_dwh.dim_proveedores (
    nombre_proveedor    VARCHAR(200)    NOT NULL,
    tipo_energia        VARCHAR(50)     NOT NULL,
    pais_origen         VARCHAR(100),
    capacidad_mw        DECIMAL(12,2),
    fecha_registro      DATE,
    activo              BOOLEAN,
    _fecha_ingesta      DATE,
    _fuente             VARCHAR(100)
)
DISTSTYLE ALL
SORTKEY (tipo_energia, activo);

-- ── Dimensión Clientes ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS energia_dwh.dim_clientes (
    tipo_identificacion         VARCHAR(10)     NOT NULL,
    identificacion              VARCHAR(50)     NOT NULL,
    identificacion_enmascarada  VARCHAR(20),
    nombre                      VARCHAR(200),
    ciudad                      VARCHAR(100),
    segmento                    VARCHAR(50),
    consumo_promedio_kwh        DECIMAL(15,2),
    fecha_vinculacion           DATE,
    antiguedad_dias             BIGINT,
    _fecha_ingesta              DATE,
    CONSTRAINT pk_clientes PRIMARY KEY (tipo_identificacion, identificacion)
)
DISTSTYLE ALL
SORTKEY (segmento, ciudad);

-- ── Tabla de Hechos Transacciones ────────────────────────────
CREATE TABLE IF NOT EXISTS energia_dwh.fact_transacciones (
    id_transaccion                  VARCHAR(50)     NOT NULL,
    tipo_transaccion                VARCHAR(20),
    nombre_cliente_proveedor        VARCHAR(200),
    cantidad_kwh                    DECIMAL(18,2),
    precio_cop_kwh                  DECIMAL(12,4),
    valor_total_cop                 DECIMAL(20,2),
    tipo_energia                    VARCHAR(50),
    fecha_transaccion               DATE,
    estado                          VARCHAR(30),
    mercado                         VARCHAR(30),
    clasificacion_volumen           VARCHAR(20),
    anio                            SMALLINT,
    mes                             SMALLINT,
    dia_semana                      SMALLINT,
    precio_promedio_tipo_energia    DECIMAL(12,4),
    sobre_precio_promedio           BOOLEAN,
    desviacion_precio_pct           DECIMAL(8,2),
    _fecha_ingesta                  DATE,
    CONSTRAINT pk_transacciones PRIMARY KEY (id_transaccion)
)
DISTKEY (tipo_energia)
SORTKEY (fecha_transaccion, tipo_transaccion);

-- ── Agregados: Métricas por tipo de energía ──────────────────
CREATE TABLE IF NOT EXISTS energia_dwh.agg_metricas_energia (
    tipo_energia            VARCHAR(50),
    tipo_transaccion        VARCHAR(20),
    num_transacciones       BIGINT,
    total_kwh               DECIMAL(20,2),
    total_cop               DECIMAL(20,2),
    precio_promedio_kwh     DECIMAL(12,4),
    precio_min_kwh          DECIMAL(12,4),
    precio_max_kwh          DECIMAL(12,4),
    _fecha_ingesta          DATE,
    _tipo_metrica           VARCHAR(30)
)
DISTSTYLE ALL
SORTKEY (_fecha_ingesta, tipo_energia);

-- ── Agregados: Métricas por segmento ────────────────────────
CREATE TABLE IF NOT EXISTS energia_dwh.agg_metricas_segmento (
    segmento                VARCHAR(50),
    tipo_energia            VARCHAR(50),
    num_transacciones       BIGINT,
    total_kwh               DECIMAL(20,2),
    total_cop               DECIMAL(20,2),
    ticket_promedio_cop     DECIMAL(20,2),
    _fecha_ingesta          DATE,
    _tipo_metrica           VARCHAR(30)
)
DISTSTYLE ALL
SORTKEY (_fecha_ingesta, segmento);

-- ── Vistas analíticas ────────────────────────────────────────

-- Vista: Margen por tipo de energía
CREATE OR REPLACE VIEW energia_dwh.v_margen_energia AS
SELECT
    c.tipo_energia,
    c.precio_promedio_kwh                                   AS precio_compra_kwh,
    v.precio_promedio_kwh                                   AS precio_venta_kwh,
    ROUND(v.precio_promedio_kwh - c.precio_promedio_kwh, 4) AS margen_cop_kwh,
    ROUND((v.precio_promedio_kwh - c.precio_promedio_kwh)
          / c.precio_promedio_kwh * 100, 2)                 AS margen_pct,
    c.total_kwh                                             AS kwh_comprado,
    v.total_kwh                                             AS kwh_vendido,
    c._fecha_ingesta
FROM energia_dwh.agg_metricas_energia c
JOIN energia_dwh.agg_metricas_energia v
  ON c.tipo_energia = v.tipo_energia
 AND c._fecha_ingesta = v._fecha_ingesta
WHERE c.tipo_transaccion = 'compra'
  AND v.tipo_transaccion = 'venta';

-- Vista: Resumen ejecutivo diario
CREATE OR REPLACE VIEW energia_dwh.v_resumen_diario AS
SELECT
    fecha_transaccion,
    tipo_energia,
    tipo_transaccion,
    COUNT(*)                        AS num_transacciones,
    ROUND(SUM(cantidad_kwh), 2)     AS total_kwh,
    ROUND(SUM(valor_total_cop), 2)  AS total_cop,
    ROUND(AVG(precio_cop_kwh), 4)   AS precio_promedio
FROM energia_dwh.fact_transacciones
WHERE estado = 'completada'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;
