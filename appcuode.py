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

# GitHub Pages publica la carpeta /public (tu workflow ya está armado así)
API_OUTPUT_BASE = os.path.join(REPO_PATH, "public", "data")
FLOW_NAME = "importscuode"  # NUEVO endpoint
API_OUTPUT_PATH = os.path.join(API_OUTPUT_BASE, FLOW_NAME)

DEFAULT_BRANCH = "main"

# ==============================================================================
# HELPERS
# ==============================================================================
def norm(txt):
    txt = "" if txt is None else str(txt)
    txt = txt.strip().lower()
    txt = "".join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )
    txt = re.sub(r"\s+", " ", txt)
    return txt

def run(cmd, cwd=None):
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or res.stdout.strip() or "Error ejecutando comando")
    return res.stdout.strip()

def find_header_row(filepath, max_rows=80, sheet_name=None):
    """
    En tus CUODE, el header real suele verse así:
    Período | Código Grupo | Grupo | Código Subgrupo | Subgrupo | Código Subpartida | Subpartida | TM (Peso Neto) | FOB | CIF
    """
    preview = pd.read_excel(filepath, sheet_name=sheet_name, header=None, nrows=max_rows, engine="openpyxl")
    for i in range(len(preview)):
        row = [norm(x) for x in preview.iloc[i].tolist()]
        has_periodo = ("periodo" in row)  # norm quita acentos
        has_cod_subpartida = ("codigo subpartida" in row)
        if has_periodo and has_cod_subpartida:
            return i
    return None

def parse_fecha_any(x):
    """
    Soporta:
      - 2020
      - 2020/01
      - 2020 / 01 - Enero
    Devuelve: YYYY-MM-01
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
            status.error("❌ No se encontraron .xlsx válidos (se ignoran archivos ~$.xlsx).")
            return False

        os.makedirs(API_OUTPUT_PATH, exist_ok=True)

        processed_files = 0
        years_written = set()

        for filepath in sorted(files):
            filename = os.path.basename(filepath)
            status.write(f"🔄 Procesando {filename}")

            try:
                # hoja 1 por defecto (en tus muestras: "Columnas")
                xls = pd.ExcelFile(filepath)
                sheet = xls.sheet_names[0] if xls.sheet_names else None

                header_idx = find_header_row(filepath, sheet_name=sheet)
                if header_idx is None:
                    status.warning(f"⚠️ No se detectó encabezado en {filename}.")
                    continue

                df = pd.read_excel(filepath, sheet_name=sheet, header=header_idx, engine="openpyxl")
                df.columns = df.columns.astype(str).str.strip()
                df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]  # elimina Unnamed

                # normalización columnas
                norm_cols = {norm(c): c for c in df.columns}

                def pick(*opts):
                    for o in opts:
                        if o in norm_cols:
                            return norm_cols[o]
                    return None

                col_periodo = pick("periodo", "periodo:")
                col_grupo_cod = pick("codigo grupo", "cod grupo", "codigo de grupo")
                col_grupo = pick("grupo")
                col_subgrupo_cod = pick("codigo subgrupo", "cod subgrupo", "codigo de subgrupo")
                col_subgrupo = pick("subgrupo")
                col_cod = pick("codigo subpartida", "codigo subpartida:")
                col_desc = pick("subpartida", "descripcion", "descripcion:")
                col_peso = pick("tm (peso neto)", "peso neto", "tm peso neto", "tm")
                col_fob = pick("fob")
                col_cif = pick("cif")

                # mínimos
                if not col_periodo or not col_cod:
                    status.warning(f"⚠️ Columnas clave faltantes en {filename}. Columnas: {list(df.columns)}")
                    continue

                rename = {col_periodo: "fecha_txt", col_cod: "cod"}
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
                df = df.dropna(subset=["fecha", "cod"]).copy()

                # cod subpartida limpio
                df["cod"] = (
                    df["cod"].astype(str)
                    .str.replace(".0", "", regex=False)
                    .str.replace(".", "", regex=False)
                    .str.strip()
                )
                df["cod"] = df["cod"].apply(lambda x: x.zfill(10) if x.isdigit() and len(x) < 10 else x)

                # label
                df["label"] = df["label"].apply(self.clean_text) if "label" in df.columns else "Desconocido"

                # numéricos
                for c in ["fob", "cif", "peso"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                # grupo/subgrupo como texto (códigos pueden ser "01", "011")
                if "grupo_cod" in df.columns:
                    df["grupo_cod"] = df["grupo_cod"].astype(str).str.strip()
                else:
                    df["grupo_cod"] = ""

                if "subgrupo_cod" in df.columns:
                    df["subgrupo_cod"] = df["subgrupo_cod"].astype(str).str.strip()
                else:
                    df["subgrupo_cod"] = ""

                if "grupo" not in df.columns:
                    df["grupo"] = ""
                if "subgrupo" not in df.columns:
                    df["subgrupo"] = ""

                # escribir por año
                years = sorted(df["fecha"].str[:4].unique())
                for yr in years:
                    sub = df[df["fecha"].str.startswith(yr)].copy()

                    out_file = os.path.join(API_OUTPUT_PATH, f"{yr}.json")
                    cols = ["fecha", "cod", "label", "grupo_cod", "grupo", "subgrupo_cod", "subgrupo", "fob", "cif", "peso"]
                    for col in cols:
                        if col not in sub.columns:
                            sub[col] = "" if col in ["grupo_cod", "grupo", "subgrupo_cod", "subgrupo"] else 0

                    sub[cols].to_json(out_file, orient="records", force_ascii=False)

                    years_written.add(yr)

                processed_files += 1

            except Exception as e:
                status.error(f"❌ Error en {filename}: {e}")

        # summary.json (listado de años + total CIF)
        if years_written:
            summary = []
            for yr in sorted(years_written, reverse=True):
                p = os.path.join(API_OUTPUT_PATH, f"{yr}.json")
                try:
                    d = pd.read_json(p)
                    total = float(d["cif"].sum()) if "cif" in d.columns else 0.0
                except Exception:
                    total = 0.0
                summary.append({"year": yr, "total_cif": round(total, 2), "file": f"{yr}.json"})

            sum_path = os.path.join(API_OUTPUT_PATH, "summary.json")
            with open(sum_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)

        status.success(f"✅ ETL CUODE completo: {processed_files} archivos procesados. Años: {len(years_written)}.")
        return processed_files > 0

    def git_push_changes(self):
        try:
            run(["git", "status"], cwd=REPO_PATH)
            run(["git", "add", os.path.join("public", "data")], cwd=REPO_PATH)

            diff = subprocess.run(
                ["git", "diff", "--cached", "--name-only"],
                cwd=REPO_PATH,
                capture_output=True,
                text=True,
            )
            if not diff.stdout.strip():
                return True, "No había cambios nuevos en public/data (nada que subir)."

            msg = f"Update CUODE APIs {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            run(["git", "commit", "-m", msg], cwd=REPO_PATH)
            run(["git", "push", "origin", DEFAULT_BRANCH], cwd=REPO_PATH)
            return True, "🚀 Cambios subidos. GitHub Actions debería desplegar Pages automáticamente."
        except Exception as e:
            return False, str(e)

# ==============================================================================
# UI
# ==============================================================================
st.sidebar.title("China Data Hub")
page = st.sidebar.radio("Menú", ["⚙️ Admin ETL CUODE", "📊 Validación (local)"])

etl = ETLCuode()

if page == "⚙️ Admin ETL CUODE":
    st.title("⚙️ ETL CUODE → JSON → GitHub Pages")

    raw_path = st.text_input("Ruta de Excels CUODE", value=DEFAULT_RAW_DATA_PATH)
    st.caption("Flujo: Excels (Mac) → public/data/importscuode → git push → GitHub Pages")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Ruta fuente (CUODE)**")
        st.code(raw_path)
    with c2:
        st.write("**Salida JSON (API)**")
        st.code(API_OUTPUT_PATH)

    if st.button("🔄 Procesar Excels CUODE y generar JSON", type="primary"):
        with st.status("Ejecutando ETL CUODE...", expanded=True) as status:
            ok = etl.run_process(status, raw_path)
        if ok:
            st.success("Listo. Ahora puedes publicar con Git push.")
        else:
            st.warning("No se procesó nada. Revisa warnings y la estructura de los archivos.")

    if st.button("☁️ Git commit + push (publicar)", type="secondary"):
        ok, msg = etl.git_push_changes()
        (st.success if ok else st.error)(msg)

elif page == "📊 Validación (local)":
    st.title("📊 Validación rápida (CUODE local)")

    summary_path = os.path.join(API_OUTPUT_PATH, "summary.json")
    if not os.path.exists(summary_path):
        st.info("No hay summary.json todavía. Ve a Admin ETL CUODE y procesa.")
        st.stop()

    summary = json.load(open(summary_path, encoding="utf-8"))
    years = [x["year"] for x in summary]
    year = st.selectbox("Año", years)

    data_path = os.path.join(API_OUTPUT_PATH, f"{year}.json")
    if not os.path.exists(data_path):
        st.warning("No existe el JSON del año seleccionado.")
        st.stop()

    df = pd.read_json(data_path)
    st.metric("Total CIF (USD)", f"${df['cif'].sum():,.0f}" if "cif" in df.columns else "$0")

    df["fecha"] = pd.to_datetime(df["fecha"])
    evol = df.groupby(df["fecha"].dt.to_period("M"))["cif"].sum().reset_index()
    evol["fecha"] = evol["fecha"].dt.to_timestamp()

    st.plotly_chart(px.area(evol, x="fecha", y="cif", title="Evolución CIF mensual (si aplica)"), use_container_width=True)

    top = (
        df.groupby(["cod", "label", "grupo", "subgrupo"])[["cif", "peso"]]
        .sum()
        .reset_index()
        .sort_values("cif", ascending=False)
        .head(50)
    )
    st.dataframe(top, hide_index=True, use_container_width=True)
