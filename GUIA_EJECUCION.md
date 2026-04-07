# Guía de Ejecución — Sin cuenta de AWS

**Tiempo estimado:** 10 minutos la primera vez, 2 minutos las siguientes.

---

## Lo que vas a hacer

1. Verificar que Python está instalado **y es la versión correcta**
2. Crear un entorno virtual aislado
3. Instalar las dependencias del proyecto
4. Ejecutar la demo del pipeline completo
5. Ejecutar los tests unitarios

---

## Paso 1 — Abrir la terminal en la carpeta correcta

Descomprime el ZIP. Abre la terminal (o la terminal integrada de VS Code) y navega hasta la carpeta raíz del proyecto — la que contiene `README.md`, `Makefile` y `demo_local.py`.

```bash
# Ejemplo en Mac/Linux
cd ~/Desktop/Ejercicio_1_Amaris_DataLake

# Ejemplo en Windows (PowerShell)
cd "C:\Users\TuNombre\Desktop\Ejercicio_1_Amaris_DataLake"
```

Confirma que estás en el lugar correcto:

```bash
ls          # Mac/Linux
dir         # Windows
```

Debes ver estos archivos listados:
```
demo_local.py   Makefile   README.md   requirements.txt   requirements-dev.txt
```

Si no los ves, revisa la ruta y repite el `cd`.

---

## Paso 2 — Verificar la versión de Python

```bash
python3 --version    # Mac/Linux
python --version     # Windows
```

Necesitas **Python 3.10 o superior**. Si ves `Python 3.10.x`, `3.11.x` o `3.12.x`, estás bien.

> **⚠ Si ves `Python 3.9` o inferior:** descarga la versión más reciente desde
> [python.org/downloads](https://python.org/downloads).
> En Windows, durante la instalación marca la casilla **"Add Python to PATH"**.
> Después cierra y vuelve a abrir la terminal antes de continuar.

---

## Paso 3 — Crear el entorno virtual

El entorno virtual es una carpeta aislada donde se instalan los paquetes del proyecto sin afectar el resto de tu computador.

```bash
# Mac/Linux
python3 -m venv .venv

# Windows
python -m venv .venv
```

Verás que se crea una carpeta llamada `.venv/`. Eso es correcto.

---

## Paso 4 — Activar el entorno virtual

Este paso cambia la terminal para que use el Python del entorno virtual en lugar del del sistema.

```bash
# Mac/Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (CMD clásico)
.venv\Scripts\activate.bat
```

Sabrás que funcionó porque el prompt de la terminal cambia y aparece `(.venv)` al inicio:

```
(.venv) PS C:\Users\tu-usuario\...\Ejercicio_1_Amaris_DataLake>
```

> **Importante:** cada vez que abras una terminal nueva, debes activar el entorno virtual de nuevo antes de ejecutar cualquier comando del proyecto.

---

## Paso 5 — Instalar las dependencias

Con el entorno virtual activo, instala todos los paquetes necesarios:

```bash
pip install -r requirements-dev.txt
```

Esto descarga e instala todos los paquetes necesarios. Tarda entre 2 y 5 minutos según la velocidad de tu conexión.

Al finalizar verás algo como:

```
Successfully installed boto3-1.34.x moto-5.x pandas-2.x pyarrow-15.x ...
```

**Verifica que la instalación quedó bien:**

```bash
python -c "import boto3, pandas, pyarrow, moto, pytest; print('OK — todo instalado')"
```

Debe responder `OK — todo instalado`. Si no, repite el paso anterior.

---

## Paso 6 — Ejecutar la demo del pipeline

Este script demuestra el pipeline completo: crea los buckets S3, carga los CSVs, aplica las tres transformaciones, convierte a Parquet, actualiza el catálogo y ejecuta las consultas analíticas. Todo en memoria, sin AWS real.

```bash
python demo_local.py
```

**Duración:** 15 a 30 segundos.

**Lo que muestra:**
- Los 5 buckets S3 creados
- Los 3 CSVs cargados con sus tamaños
- Transformación 1: normalización de proveedores y clientes
- Transformación 2: valor_total_cop calculado, clasificación por volumen
- Transformación 3: métricas agregadas por tipo de energía y segmento
- 5 tablas Parquet escritas con compresión Snappy
- 5 tablas registradas en el catálogo Glue
- Resultados de Q1, Q3, Q4 y Q5 (margen de negocio)

---

## Paso 7 — Ejecutar los tests unitarios

```bash
pytest tests/ -v
```

Verás cada test listado con su resultado. Al final aparece el resumen:

```
======= 39 passed in 2.45s =======
```

Para ver también la cobertura de código:

```bash
pytest tests/ -v --cov=scripts --cov=glue_jobs --cov=lambda --cov-report=term-missing
```

---

## Solución de problemas comunes

### ❌ Error en Windows: "Failed to build numpy" o "Unknown compiler cl.exe / gcc"

Este error aparece cuando pip intenta compilar una librería desde el código fuente y Windows no tiene un compilador C instalado.

**Solución en un solo comando:**

```bash
pip install -r requirements-dev.txt --prefer-binary
```

La opción `--prefer-binary` le indica a pip que descargue siempre el wheel precompilado disponible para tu versión de Python y Windows, sin intentar compilar nada. Si el error persiste, ejecuta esto en su lugar:

```bash
pip install --prefer-binary boto3 pandas pyarrow moto[s3,glue,athena,lambda,sqs,kms] pytest pytest-cov colorama python-dotenv tqdm black flake8 isort boto3-stubs
```

---

### ❌ "python3: command not found" en Windows

Usa `python` en lugar de `python3` en todos los comandos. En Windows el ejecutable se llama simplemente `python`.

---

### ❌ "No module named 'moto'" o similar después de instalar

El entorno virtual no está activo. Ejecuta el comando de activación del Paso 4 y vuelve a intentar.

---

### ❌ "No such file or directory: data/sample/proveedores.csv"

Estás ejecutando el script desde la carpeta equivocada. Asegúrate de estar en la raíz del proyecto (la que contiene `demo_local.py`), no dentro de una subcarpeta.

---

### ❌ ".venv\Scripts\Activate.ps1 cannot be loaded" en Windows PowerShell

PowerShell bloquea la ejecución de scripts por política de seguridad. Ejecuta esto una sola vez:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Luego vuelve a intentar el comando de activación.

---

### ❌ Los tests fallan con error de importación

Verifica que estás en la carpeta raíz del proyecto y que el entorno virtual está activo. Luego ejecuta:

```bash
pip install -r requirements-dev.txt --prefer-binary --force-reinstall
```

---

## Resultado esperado al ejecutar la demo

```
══════════════════════════════════════════════════════════
  DATA LAKE — COMERCIALIZADORA DE ENERGÍA
  Demo local  |  Prueba Técnica Amaris
══════════════════════════════════════════════════════════

FASE 1/6  Creación de infraestructura S3 y catálogo Glue
  ✅  Bucket landing:   s3://amaris-dev-landing
  ✅  Bucket raw:       s3://amaris-dev-raw
  ✅  Bucket processed: s3://amaris-dev-processed
  ✅  Bucket curated:   s3://amaris-dev-curated
  ✅  Catálogo Glue:    amaris_datalake_dev_processed

FASE 2/6  Ingesta de CSVs → Landing Zone
  ✅  proveedores.csv → 10 filas
  ✅  clientes.csv → 10 filas
  ✅  transacciones.csv → 15 filas

FASE 3/6  Transformaciones ETL
  ✅  T1 Proveedores: normalizado, duplicados eliminados
  ✅  T1 Clientes: identificaciones enmascaradas ****XXXX
  ✅  T2 Transacciones: valor_total_cop calculado
  ✅  T3 Métricas: agregados por energía y segmento

FASE 4/6  Parquet en Processed Zone
  ✅  proveedores.parquet    ✅  clientes.parquet
  ✅  transacciones.parquet  ✅  metricas_energia.parquet
  ✅  metricas_segmento.parquet

FASE 5/6  Catálogo Glue
  ✅  5 tablas catalogadas con particiones year/month/day

FASE 6/6  Consultas analíticas
  Q5 — Margen compra vs venta:
  | tipo_energia   | precio_compra | precio_venta | margen_pct |
  | nuclear        | 207.75        | 430.0        | 107.0 %    |
  | hidroelectrica | 259.0         | 391.5        |  51.2 %    |
  | eolica         | 283.5         | 411.0        |  45.0 %    |

PIPELINE COMPLETO — RESUMEN
  ✅  Tiempo total: ~20 segundos
  ✅  Sin cuenta AWS. Sin Docker. Sin configuración.
```

---

## Resultado esperado al ejecutar los tests

```
tests/test_etl.py::TestNormalizacionProveedores::test_elimina_espacios PASSED
tests/test_etl.py::TestNormalizacionProveedores::test_tipo_energia_minusculas PASSED
...
tests/test_etl.py::TestDatosDeMuestra::test_precios_son_positivos PASSED

======= 39 passed in 2.45s =======
```

---

## Lo que vas a hacer

1. Verificar que Python está instalado
2. Crear un entorno virtual aislado
3. Instalar las dependencias del proyecto
4. Ejecutar la demo del pipeline completo
5. Ejecutar los tests unitarios

---

## Paso 1 — Abrir la terminal en la carpeta correcta

Descomprime el ZIP. Abre la terminal (o la terminal integrada de VS Code) y navega hasta la carpeta raíz del proyecto — la que contiene `README.md`, `Makefile` y `demo_local.py`.

```bash
# Ejemplo en Mac/Linux
cd ~/Desktop/Ejercicio_1_Amaris_DataLake

# Ejemplo en Windows (PowerShell)
cd C:\Users\TuNombre\Desktop\Ejercicio_1_Amaris_DataLake
```

Confirma que estás en el lugar correcto:

```bash
ls          # Mac/Linux
dir         # Windows
```

Debes ver estos archivos listados:
```
demo_local.py   Makefile   README.md   requirements.txt   requirements-dev.txt
```

Si no los ves, revisa la ruta y repite el `cd`.

---

## Paso 2 — Verificar la versión de Python

```bash
python3 --version    # Mac/Linux
python --version     # Windows
```

Necesitas **Python 3.10 o superior**. Si ves `Python 3.10.x`, `3.11.x` o `3.12.x`, estás bien.

Si ves `Python 2.7` o un error, descarga Python desde [python.org/downloads](https://python.org/downloads) e instálalo. Al instalar en Windows, marca la casilla **"Add Python to PATH"**.

---

## Paso 3 — Crear el entorno virtual

El entorno virtual es una carpeta aislada donde se instalan los paquetes del proyecto sin afectar el resto de tu computador.

```bash
# Mac/Linux
python3 -m venv .venv

# Windows
python -m venv .venv
```

Verás que se crea una carpeta llamada `.venv/`. Eso es correcto.

---

## Paso 4 — Activar el entorno virtual

Este paso cambia la terminal para que use el Python del entorno virtual en lugar del del sistema.

```bash
# Mac/Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (CMD)
.venv\Scripts\activate.bat
```

Sabrás que funcionó porque el prompt de la terminal cambia y aparece `(.venv)` al inicio:

```
(.venv) tu-usuario@tu-computador:~/Ejercicio_1_Amaris_DataLake$
```

> **Importante:** cada vez que abras una terminal nueva, debes activar el entorno virtual de nuevo antes de ejecutar cualquier comando del proyecto.

---

## Paso 5 — Instalar las dependencias

Con el entorno virtual activo, instala todos los paquetes necesarios:

```bash
pip install -r requirements-dev.txt
```

Verás que se descarga e instala una lista de paquetes. Esto tarda entre 2 y 5 minutos según la velocidad de tu conexión a internet. Al finalizar verás algo como:

```
Successfully installed boto3-1.34.69 moto-5.0.3 pandas-2.2.1 pyarrow-15.0.2 ...
```

Verifica que la instalación quedó bien:

```bash
python -c "import boto3, pandas, pyarrow, moto, pytest; print('OK')"
```

Debe responder `OK`. Si responde con un error de módulo no encontrado, repite el paso anterior.

---

## Paso 6 — Ejecutar la demo del pipeline

Este script demuestra el pipeline completo: crea los buckets S3, carga los CSVs, aplica las tres transformaciones, convierte a Parquet, actualiza el catálogo y ejecuta las consultas analíticas. Todo en memoria, sin AWS real.

```bash
python demo_local.py
```

Verás el progreso fase por fase y al final los resultados de las 4 consultas analíticas.

**Duración:** 15 a 30 segundos.

**Lo que muestra:**
- Los 5 buckets S3 creados
- Los 3 CSVs cargados con sus tamaños
- Transformación 1: normalización de proveedores y clientes
- Transformación 2: valor_total_cop calculado, clasificación por volumen
- Transformación 3: métricas agregadas por tipo de energía y segmento
- 5 tablas Parquet escritas con compresión Snappy
- 5 tablas registradas en el catálogo Glue
- Resultados de Q1, Q3, Q4 y Q5 (margen de negocio)

---

## Paso 7 — Ejecutar los tests unitarios

Los tests validan la lógica de las tres transformaciones, la construcción de claves S3, el enmascaramiento de identificaciones y la integridad de los archivos CSV. No necesitan AWS real.

```bash
pytest tests/ -v
```

Verás cada test listado con su resultado. Al final aparece el resumen:

```
======= 39 passed in 2.45s =======
```

Para ver además la cobertura de código (qué porcentaje del código está cubierto por tests):

```bash
pytest tests/ -v --cov=scripts --cov=glue_jobs --cov=lambda --cov-report=term-missing
```

---

## Comandos alternativos con Make

Si tienes `make` instalado (viene por defecto en Mac y Linux; en Windows instala con `winget install GnuWin32.Make`), puedes usar los atajos del Makefile:

```bash
make setup      # Equivale a los pasos 3, 4 y 5 juntos
make test       # Equivale al paso 7
```

---

## Solución de problemas comunes

**"python3: command not found" en Windows**

Usa `python` en lugar de `python3` en todos los comandos. En Windows el ejecutable se llama simplemente `python`.

---

**"No module named 'moto'" o similar después de instalar**

El entorno virtual no está activo. Ejecuta el comando de activación del Paso 4 y vuelve a intentar.

---

**"No such file or directory: data/sample/proveedores.csv"**

Estás ejecutando el script desde la carpeta equivocada. Asegúrate de estar en la raíz del proyecto (la que contiene `demo_local.py`), no dentro de una subcarpeta.

---

**".venv\Scripts\Activate.ps1 cannot be loaded" en Windows PowerShell**

PowerShell bloquea la ejecución de scripts por política de seguridad. Ejecuta esto una sola vez:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Luego vuelve a intentar el comando de activación.

---

**Los tests fallan con error de importación**

Verifica que estás en la carpeta raíz del proyecto y que el entorno virtual está activo. Luego ejecuta:

```bash
pip install -r requirements-dev.txt --force-reinstall
```

---

## Resultado esperado al ejecutar la demo

```
══════════════════════════════════════════════════════════
  DATA LAKE — COMERCIALIZADORA DE ENERGÍA
  Demo local  |  Prueba Técnica Amaris
══════════════════════════════════════════════════════════

FASE 1/6  Creación de infraestructura S3 y catálogo Glue
  ✅  Bucket landing:   s3://amaris-dev-landing
  ✅  Bucket raw:       s3://amaris-dev-raw
  ✅  Bucket processed: s3://amaris-dev-processed
  ✅  Bucket curated:   s3://amaris-dev-curated
  ✅  Catálogo Glue:    amaris_datalake_dev_processed

FASE 2/6  Ingesta de CSVs → Landing Zone (Lambda)
  ✅  proveedores.csv → 10 filas, 413 bytes
  ✅  clientes.csv → 10 filas, 571 bytes
  ✅  transacciones.csv → 15 filas, 1.102 bytes

...

FASE 6/6  Consultas analíticas
  Q5 — Margen estimado compra vs venta por tipo de energía
  +----------------+-------------------+------------------+----------------+-----------+
  | tipo_energia   | precio_compra_kwh | precio_venta_kwh | margen_cop_kwh | margen_pct|
  +----------------+-------------------+------------------+----------------+-----------+
  | nuclear        | 207.75            | 430.0            | 222.25         | 107.0     |
  | eolica         | 283.5             | 411.0            | 127.5          | 44.97     |
  | hidroelectrica | 259.0             | 391.5            | 132.5          | 51.16     |
  +----------------+-------------------+------------------+----------------+-----------+

══════════════════════════════════════════════════════════
  PIPELINE COMPLETO — RESUMEN
  ✅  Buckets S3 creados:          5
  ✅  CSVs ingeridos:              3 archivos
  ✅  Transformaciones aplicadas:  3
  ✅  Tablas Parquet en Processed: 5
  ✅  Tablas en catálogo Glue:     5
  ✅  Consultas SQL ejecutadas:    4 (Q1, Q3, Q4, Q5)
  ✅  Tiempo total:                18.3 segundos

  Sin cuenta AWS. Sin Docker. Sin configuración.
  Todo simulado en memoria con moto + pandas + pyarrow.
```

---

## Resultado esperado al ejecutar los tests

```
tests/test_etl.py::TestNormalizacionProveedores::test_elimina_espacios_en_nombre PASSED
tests/test_etl.py::TestNormalizacionProveedores::test_tipo_energia_en_minusculas PASSED
tests/test_etl.py::TestNormalizacionProveedores::test_filtra_tipo_energia_invalido PASSED
...
tests/test_etl.py::TestDatosDeMuestra::test_precios_son_positivos PASSED

======= 39 passed in 2.45s =======
```
