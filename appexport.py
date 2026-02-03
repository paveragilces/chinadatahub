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
st.set_page_config(layout="wide", page_title="China Exports ETL", page_icon="🇨🇳")

# Por defecto, reutiliza la misma carpeta que tu app original.
# Puedes cambiarla desde el UI si lo necesitas.
DEFAULT_RAW_DATA_PATH = "/Users/paulvera/Desktop/China Data Hub/Subpartidas"

REPO_PATH = os.getcwd()
API_OUTPUT_PATH = os.path.join(REPO_PATH, "public", "data", "exports")
DEFAULT_BRANCH = "main"  # cambia a "master" si tu repo usa master


# ==============================================================================
# HELPERS
# ==============================================================================

def norm(txt):
    txt = "" if txt is None else str(txt)
    txt = txt.strip().lower()
    txt = "".join(
        c
        for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )
    txt = re.sub(r"\s+", " ", txt)
    return txt


def find_header_row(filepath, max_rows=40):
    preview = pd.read_excel(filepath, header=None, nrows=max_rows, engine="openpyxl")
    for i in range(len(preview)):
        row = [norm(x) for x in preview.iloc[i].tolist()]
        if "codigo subpartida" in row and "periodo" in row:
            return i
    return None


def run(cmd, cwd=None):
    """Ejecuta comandos y retorna stdout (o lanza error)."""
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or res.stdout.strip() or "Error ejecutando comando")
    return res.stdout.strip()


def is_export_file(filename: str) -> bool:
    """Archivos esperados: 2000-export-china-subpartidas.xlsx (case-insensitive)."""
    base = os.path.basename(filename).lower()
    return bool(re.search(r"\bexport\b", base))


# ==============================================================================
# ETL ENGINE (solo exports)
# ==============================================================================

class ExportETL:
    def clean_text(self, text):
        if not isinstance(text, str):
            return "Desconocido"
        txt = text.strip().upper()
        for p in [r"^LOS DEMÁS\s*", r"^LAS DEMÁS\s*", r"^OTROS\s*", r"^OTRAS\s*", r"\(.*?\)"]:
            txt = re.sub(p, "", txt)
        txt = txt.strip()
        return txt.capitalize() if txt else "Desconocido"

    def get_sector(self, code):
        cap = str(code)[:2]
        sectors = {
            "03": "🦐 Pesca",
            "07": "🥦 Hortalizas",
            "08": "🍌 Frutas",
            "16": "🥫 Conservas",
            "18": "🍫 Cacao",
            "29": "🧪 Químicos",
            "30": "💊 Farma",
            "39": "🧴 Plásticos",
            "44": "🪵 Madera",
            "72": "🏗️ Hierro/Acero",
            "84": "⚙️ Maquinaria",
            "85": "🔌 Eléctrico",
            "87": "🚗 Vehículos",
        }
        return sectors.get(cap, "📦 Otros")

    def run_process(self, raw_data_path: str, status):
        status.write(f"📂 Leyendo: `{raw_data_path}`")

        files = [
            f
            for f in glob.glob(os.path.join(raw_data_path, "*.xlsx"))
            if (not os.path.basename(f).startswith("~$")) and is_export_file(f)
        ]

        if not files:
            status.error(
                "❌ No se encontraron .xlsx de exportación (se esperan nombres tipo: 2000-export-china-subpartidas.xlsx)."
            )
            return False

        processed = 0
        resumen = []

        os.makedirs(API_OUTPUT_PATH, exist_ok=True)

        for filepath in sorted(files):
            filename = os.path.basename(filepath)
            status.write(f"🔄 {filename}")

            try:
                header_idx = find_header_row(filepath)
                if header_idx is None:
                    status.warning(
                        f"⚠️ No se detectó encabezado (Período + Código Subpartida) en {filename}"
                    )
                    continue

                df = pd.read_excel(filepath, header=header_idx, dtype=str, engine="openpyxl")
                df.columns = df.columns.astype(str).str.strip()
                df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]

                # columnas normalizadas
                norm_cols = {norm(c): c for c in df.columns}

                def pick(*opts):
                    for o in opts:
                        if o in norm_cols:
                            return norm_cols[o]
                    return None

                rename = {}
                rename[pick("periodo", "período")] = "fecha_txt"
                rename[pick("pais origen", "país origen", "pais de origen", "país de origen")] = "pais"
                rename[pick("codigo subpartida", "código subpartida")] = "cod"
                rename[pick("subpartida", "descripcion", "descripción")] = "desc"
                rename[pick("tm (peso neto)", "peso neto")] = "peso"
                rename[pick("fob")] = "fob"
                rename[pick("cif")] = "cif"  # a veces viene, pero en exports usamos FOB
                rename = {k: v for k, v in rename.items() if k is not None}

                df = df.rename(columns=rename)

                if "cod" not in df.columns or "fecha_txt" not in df.columns:
                    status.warning(
                        f"⚠️ Faltan columnas clave en {filename}. Halladas: {list(df.columns)}"
                    )
                    continue

                # Fecha "YYYY / MM - Mes" -> "YYYY-MM-01"
                def parse_fecha(txt):
                    m = re.search(r"(\d{4})\s*/\s*(\d{2})", str(txt))
                    return f"{m.group(1)}-{m.group(2)}-01" if m else None

                df["fecha"] = df["fecha_txt"].apply(parse_fecha)
                df = df.dropna(subset=["fecha", "cod"])

                # numéricos
                for c in ["fob", "cif", "peso"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                df["cod"] = (
                    df["cod"].astype(str).str.replace(".", "", regex=False).str.strip().str.zfill(10)
                )
                df["sector"] = df["cod"].apply(self.get_sector)
                df["label"] = df["desc"].apply(self.clean_text) if "desc" in df.columns else "Desconocido"

                years = sorted(df["fecha"].str[:4].unique())

                for yr in years:
                    sub = df[df["fecha"].str.startswith(yr)].copy()
                    cols = ["fecha", "cod", "label", "sector", "fob", "cif", "peso"]
                    for c in cols:
                        if c not in sub.columns:
                            sub[c] = 0 if c in ["fob", "cif", "peso"] else ""

                    out_path = os.path.join(API_OUTPUT_PATH, f"{yr}.json")
                    sub[cols].to_json(out_path, orient="records", force_ascii=False)

                    total = sub["fob"].sum()
                    resumen = [x for x in resumen if x["year"] != yr]
                    resumen.append({"year": yr, "total": round(float(total), 2), "file": f"{yr}.json"})

                processed += 1

            except Exception as e:
                status.error(f"❌ Error en {filename}: {e}")

        if resumen:
            resumen.sort(key=lambda x: x["year"], reverse=True)
            sum_path = os.path.join(API_OUTPUT_PATH, "summary.json")
            with open(sum_path, "w", encoding="utf-8") as f:
                json.dump(resumen, f, ensure_ascii=False)

        status.success(f"✅ ETL exports completo: {processed} archivos procesados.")
        return processed > 0

    def git_push_changes(self):
        try:
            run(["git", "status"], cwd=REPO_PATH)

            # add + commit (solo si hay cambios)
            run(["git", "add", "public/data/exports"], cwd=REPO_PATH)

            diff = subprocess.run(
                ["git", "diff", "--cached", "--name-only"],
                cwd=REPO_PATH,
                capture_output=True,
                text=True,
            )
            if not diff.stdout.strip():
                return True, "No había cambios nuevos en public/data/exports (nada que subir)."

            msg = f"Update exports APIs {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            run(["git", "commit", "-m", msg], cwd=REPO_PATH)
            run(["git", "push", "origin", DEFAULT_BRANCH], cwd=REPO_PATH)
            return True, "🚀 Cambios subidos. GitHub Pages debería desplegar automáticamente."

        except Exception as e:
            return False, str(e)

# ==============================================================================
# UI
# ==============================================================================
st.sidebar.title("China Exports")
page = st.sidebar.radio("Menú", ["⚙️ Admin ETL (exports)", "📊 Dashboard (exports)"])

raw_data_path = st.sidebar.text_input("Ruta de Excels (exports)", value=DEFAULT_RAW_DATA_PATH)

etl = ExportETL()

if page == "⚙️ Admin ETL (exports)":
    st.title("⚙️ Exports ETL → JSON → GitHub Pages")
    st.caption(
        "Flujo: procesas Excels (solo export) → genera public/data/exports → git push → Pages despliega."
    )

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Ruta de Excels**")
        st.code(raw_data_path)
        st.write("**Filtro aplicado**")
        st.code("*.xlsx que contengan la palabra 'export' (ej: 2000-export-china-subpartidas.xlsx)")
    with c2:
        st.write("**Salida JSON**")
        st.code(API_OUTPUT_PATH)

    if st.button("🔄 Procesar Excels de exportación y generar JSON", type="primary"):
        with st.status("Ejecutando ETL (exports)...", expanded=True) as status:
            ok = etl.run_process(raw_data_path, status)
        if ok:
            st.success("Listo. Ahora puedes subir a GitHub.")

    if st.button("☁️ Git commit + push (publicar)", type="secondary"):
        ok, msg = etl.git_push_changes()
        (st.success if ok else st.error)(msg)

elif page == "📊 Dashboard (exports)":
    st.title("📊 Validación rápida (exports)")

    summary_path = os.path.join(API_OUTPUT_PATH, "summary.json")
    if not os.path.exists(summary_path):
        st.info("No hay summary.json aún. Ve a Admin ETL y procesa los Excels.")
        st.stop()

    years = [x["year"] for x in json.load(open(summary_path, encoding="utf-8"))]
    year = st.selectbox("Año", years)

    data_path = os.path.join(API_OUTPUT_PATH, f"{year}.json")
    if not os.path.exists(data_path):
        st.warning("No existe el JSON del año seleccionado.")
        st.stop()

    df = pd.read_json(data_path)
    st.metric("Total FOB USD", f"${df['fob'].sum():,.0f}")

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])

    evol = df.groupby(df["fecha"].dt.to_period("M"))["fob"].sum().reset_index()
    evol["fecha"] = evol["fecha"].dt.to_timestamp()

    st.plotly_chart(
        px.area(evol, x="fecha", y="fob", title="Evolución mensual (FOB)"),
        use_container_width=True,
    )

    top = (
        df.groupby(["cod", "label"])[["fob", "peso"]]
        .sum()
        .reset_index()
        .sort_values("fob", ascending=False)
        .head(50)
    )
    st.dataframe(top, hide_index=True, use_container_width=True)
