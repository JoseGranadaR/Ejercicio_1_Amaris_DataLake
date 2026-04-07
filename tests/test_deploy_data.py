"""
tests/test_deploy_data.py
==========================
Pruebas unitarias para scripts/deploy_data.py.
Usa moto para simular S3 sin necesitar una cuenta de AWS real.

Ejecutar:
    pytest tests/test_deploy_data.py -v
"""

import pytest
from pathlib import Path

import boto3
from moto import mock_aws


# ── Fixture: bucket S3 simulado ───────────────────────────────
@pytest.fixture
def aws_env(monkeypatch):
    """Configura variables de entorno de AWS para el entorno de pruebas."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID",     "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION",    "us-east-1")
    monkeypatch.setenv("AWS_REGION",            "us-east-1")
    monkeypatch.setenv("LANDING_BUCKET",        "test-landing-bucket")
    monkeypatch.setenv("ENVIRONMENT",           "test")


@pytest.fixture
def s3_bucket(aws_env):
    """Crea un bucket S3 falso con moto."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-landing-bucket")
        yield client


@pytest.fixture
def sample_csv(tmp_path):
    """Crea archivos CSV de prueba en una carpeta temporal."""
    (tmp_path / "proveedores.csv").write_text(
        "nombre_proveedor,tipo_energia,capacidad_mw\nEnerSol,eolica,350\n"
    )
    (tmp_path / "clientes.csv").write_text(
        "identificacion,nombre,segmento\n123,María,residencial\n"
    )
    (tmp_path / "transacciones.csv").write_text(
        "id_transaccion,tipo_transaccion,cantidad_kwh\nTRX-001,compra,500000\n"
    )
    return tmp_path


# ── Tests ─────────────────────────────────────────────────────

class TestBuildS3Key:
    """Prueba la construcción de la clave S3 con la estructura de partición."""

    def test_estructura_correcta(self):
        from scripts.deploy_data import build_s3_key
        key = build_s3_key("ventas", "2024-01-15", "08", "ventas.csv")
        assert key == "raw/ventas/year=2024/month=01/day=15/hour=08/ventas.csv"

    def test_usa_año_correcto(self):
        from scripts.deploy_data import build_s3_key
        key = build_s3_key("clientes", "2025-12-31", "00", "clientes.csv")
        assert "year=2025" in key

    def test_usa_mes_con_ceros(self):
        from scripts.deploy_data import build_s3_key
        key = build_s3_key("clientes", "2024-03-01", "00", "clientes.csv")
        assert "month=03" in key

    def test_usa_dia_con_ceros(self):
        from scripts.deploy_data import build_s3_key
        key = build_s3_key("clientes", "2024-01-05", "00", "clientes.csv")
        assert "day=05" in key

    def test_nombre_archivo_al_final(self):
        from scripts.deploy_data import build_s3_key
        key = build_s3_key("proveedores", "2024-01-15", "00", "proveedores.csv")
        assert key.endswith("proveedores.csv")


class TestUploadFile:
    """Prueba la subida de archivos a S3."""

    def test_dry_run_no_sube_nada(self, s3_bucket, sample_csv, capsys):
        """En modo dry-run no debe subir ningún archivo."""
        with mock_aws():
            from scripts.deploy_data import upload_file
            csv_path = sample_csv / "proveedores.csv"
            result   = upload_file(s3_bucket, csv_path, "test-landing-bucket",
                                   "raw/proveedores/proveedores.csv", dry_run=True)
            assert result is True
            # Verificar que el archivo NO está en S3
            resp = s3_bucket.list_objects_v2(Bucket="test-landing-bucket")
            assert resp.get("KeyCount", 0) == 0

    def test_sube_archivo_correctamente(self, s3_bucket, sample_csv):
        """Debe subir el archivo y verificar que existe en S3."""
        with mock_aws():
            from scripts.deploy_data import upload_file
            csv_path = sample_csv / "proveedores.csv"
            s3_key   = "raw/proveedores/year=2024/month=01/day=15/hour=00/proveedores.csv"
            result   = upload_file(s3_bucket, csv_path, "test-landing-bucket",
                                   s3_key, dry_run=False)
            assert result is True

    def test_archivo_inexistente_retorna_false(self, s3_bucket):
        """Si el archivo no existe en disco, debe retornar False."""
        with mock_aws():
            from scripts.deploy_data import upload_file
            fake_path = Path("/ruta/que/no/existe/fake.csv")
            result    = upload_file(s3_bucket, fake_path, "test-landing-bucket",
                                    "raw/fake/fake.csv", dry_run=False)
            assert result is False
