import streamlit as st
import pandas as pd
import json
import os
import re
import glob
import subprocess
import unicodedata
import plotly.express as px
from datetime import datetime

# ==============================================================================
# CONFIG
# ==============================================================================
st.set_page_config(layout="wide", page_title="China Data Hub ETL - CUODE", page_icon="🇨🇳")

DEFAULT_RAW_DATA_PATH = "/Users/paulvera/Desktop/China Data Hub/Cuode"
REPO_PATH = os.getcwd()
API_OUTPUT_BASE = os.path.join(REPO_PATH, "public", "data")
FLOW_NAME = "importscuode"  # NUEVO endpoint
API_OUTPUT_PATH = os.path.join(API_OUTPUT_BASE, FLOW_NAME)
DEFAULT_BRANCH = "main"  # cambia si tu repo usa master


# ==============================================================================
# HELPERS
# ==============================================================================
def norm(txt):
    txt = "" if txt is None else str(txt)
    txt = txt.strip().lower()
    txt = ''.join(c for c in unicodedata.normalize("NFD", txt)
                  if unicodedata.category(c) != "Mn")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def run(cmd, cwd=None):
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or res.stdout.strip() or "Error ejecutando comando")
    return res.stdout.strip()


def find_header_row(filepath, max_rows=60, sheet_name=None):
    """
    Detecta fila de header buscando combinaciones típicas CUODE:
    - periodo/período
    - codigo subpartida
    - (opcional) codigo grupo / codigo subgrupo
    """
    preview = pd.read_excel(filepath, sheet_name=sheet_name, header=None, nrows=max_rows, engine="openpyxl")
    for i in range(len(preview)):
        row = [norm(x) for x in preview.iloc[i].tolist()]
        has_periodo = ("periodo" in row) or ("período" in row)  # por si acaso
        has_cod_subpartida = any("codigo subpartida" == c or "código subpartida" == c for c in row) or ("codigo subpartida" in row)
        if has_periodo and has_cod_subpartida:
            return i
    return None


def parse_fecha_any(x):
    """
    CUODE suele venir anual (2020), pero soportamos:
      - 'YYYY'
      - 'YYYY/MM'
      - 'YYYY / MM - Mes'
    Devuelve 'YYYY-MM-01'
    """
    s = str(x).strip()

    m = re.search(r"(\d{4})\s*/\s*(\d{1,2})", s)
    if m:
        yyyy = m.group(1)
        mm = int(m.group(2))
        return f"{yyyy}-{mm:02d}-01"

    m2 = re.search(r"(\d{4})", s)
    if m2:
        yyyy = m2.group(1)
        return f"{yyyy}-01-01"

    return None


# ==============================================================================
# ETL ENGINE (CUODE)
# ==============================================================================
class ETLCuode:

    def clean_text(self, text):
        if not isinstance(text, str):
            return "Desconocido"
        txt = text.strip()
        txt = re.sub(r"\(.*?\)", "", txt).strip()
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt if txt else "Desconocido"

    def run_process(self, status, raw_path):
        status.write(f"📂 Leyendo CUODE desde: `{raw_path}`")

        files = [
            f for f in glob.glob(os.path.join(raw_path, "*.xlsx"))
            if not os.path.basename(f).startswith("~$")
        ]

        if not files:
            status.error("❌ No se encontraron .xlsx válidos (se ignoran ~$.xlsx).")
            return False

        os.makedirs(API_OUTPUT_PATH, exist_ok=True)
        resumen = []
        processed = 0

        for filepath in sorted(files):
            filename = os.path.basename(filepath)
            status.write(f"🔄 Procesando {filename}")

            try:
                # por si viene con hoja distinta, usamos la primera
                xls = pd.ExcelFile(filepath)
                sheet = xls.sheet_names[0] if xls.sheet_names else None

                header_idx = find_header_row(filepath, sheet_name=sheet)
                if header_idx is None:
                    status.warning(f"⚠️ No se detectó encabezado en {filename} (Período + Código Subpartida).")
                    continue

                df = pd.read_excel(filepath, sheet_name=sheet, header=header_idx, engine="openpyxl")
                df.columns = df.columns.astype(str).str.strip()
                df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]  # quita Unnamed

                # normalización columnas
                norm_cols = {norm(c): c for c in df.columns}

                def pick(*opts):
                    for o in opts:
                        if o in norm_cols:
                            return norm_cols[o]
                    return None

                # CUODE: NO hay país. Sí hay grupo/subgrupo.
                col_periodo = pick("periodo", "período")
                col_grupo_cod = pick("codigo grupo", "código grupo", "cod grupo", "codigo de grupo")
                col_grupo = pick("grupo")
                col_subgrupo_cod = pick("codigo subgrupo", "código subgrupo", "cod subgrupo", "codigo de subgrupo")
                col_subgrupo = pick("subgrupo")
                col_cod = pick("codigo subpartida", "código subpartida")
                col_desc = pick("subpartida", "descripcion", "descripción")
                col_peso = pick("tm (peso neto)", "peso neto", "tm peso neto", "tm")
                col_fob = pick("fob")
                col_cif = pick("cif")

                # columnas mínimas CUODE
                if not col_periodo or not col_cod:
                    status.warning(f"⚠️ Columnas mínimas faltantes en {filename}. Columnas: {list(df.columns)}")
                    continue

                # renombrar a esquema estándar CUODE
                rename = {
                    col_periodo: "fecha_txt",
                    col_cod: "cod",
                }
                if col_desc: rename[col_desc] = "label"
                if col_grupo_cod: rename[col_grupo_cod] = "grupo_cod"
                if col_grupo: rename[col_grupo] = "grupo"
                if col_subgrupo_cod: rename[col_subgrupo_cod] = "subgrupo_cod"
                if col_subgrupo: rename[col_subgrupo] = "subgrupo"
                if col_peso: rename[col_peso] = "peso"
                if col_fob: rename[col_fob] = "fob"
                if col_cif: rename[col_cif] = "cif"

                df = df.rename(columns=rename)

                # fecha
                df["fecha"] = df["fecha_txt"].apply(parse_fecha_any)
                df = df.dropna(subset=["fecha", "cod"])

                # cod subpartida a 10 dígitos (si viene numérico largo)
                df["cod"] = df["cod"].astype(str).str.replace(".0", "", regex=False).str.replace(".", "", regex=False).str.strip()
                # si viene 9 dígitos, zfill; si viene 10+ se respeta
                df["cod"] = df["cod"].apply(lambda x: x.zfill(10) if x.isdigit() and len(x) < 10 else x)

                # texto limpio
                if "label" in df.columns:
                    df["label"] = df["label"].apply(self.clean_text)
                else:
                    df["label"] = "Desconocido"

                for c in ["fob", "cif", "peso", "grupo_cod", "subgrupo_cod"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)




