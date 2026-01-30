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
st.set_page_config(layout="wide", page_title="China Data Hub ETL", page_icon="🇨🇳")

RAW_DATA_PATH = "/Users/paulvera/Desktop/China Data Hub/Subpartidas"  # <-- cambia si aplica
REPO_PATH = os.getcwd()
API_OUTPUT_PATH = os.path.join(REPO_PATH, "public", "data")
DEFAULT_BRANCH = "main"  # cambia a "master" si tu repo usa master


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


# ==============================================================================
# ETL ENGINE
# ==============================================================================
class ETLEngine:

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
            '03': '🦐 Pesca', '07': '🥦 Hortalizas', '08': '🍌 Frutas',
            '16': '🥫 Conservas', '18': '🍫 Cacao',
            '29': '🧪 Químicos', '30': '💊 Farma',
            '39': '🧴 Plásticos', '44': '🪵 Madera',
            '72': '🏗️ Hierro/Acero',
            '84': '⚙️ Maquinaria', '85': '🔌 Eléctrico',
            '87': '🚗 Vehículos'
        }
        return sectors.get(cap, '📦 Otros')

    def run_process(self, status):
        status.write(f"📂 Leyendo: `{RAW_DATA_PATH}`")

        files = [
            f for f in glob.glob(os.path.join(RAW_DATA_PATH, "*.xlsx"))
            if not os.path.basename(f).startswith("~$")
        ]

        if not files:
            status.error("❌ No se encontraron .xlsx válidos (ojo: se ignoran ~$.xlsx).")
            return False

        resumen = {"imports": [], "exports": []}
        processed = 0

        os.makedirs(API_OUTPUT_PATH, exist_ok=True)

        for filepath in sorted(files):
            filename = os.path.basename(filepath)
            status.write(f"🔄 {filename}")

            tipo = "exports" if "export" in filename.lower() else "imports"

            try:
                header_idx = find_header_row(filepath)
                if header_idx is None:
                    status.warning(f"⚠️ No se detectó encabezado (Período + Código Subpartida) en {filename}")
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
                rename[pick("cif")] = "cif"
                rename = {k: v for k, v in rename.items() if k is not None}

                df = df.rename(columns=rename)

                if "cod" not in df.columns or "fecha_txt" not in df.columns:
                    status.warning(f"⚠️ Faltan columnas clave en {filename}. Halladas: {list(df.columns)}")
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

                df["cod"] = df["cod"].astype(str).str.replace(".", "", regex=False).str.strip().str.zfill(10)
                df["sector"] = df["cod"].apply(self.get_sector)
                df["label"] = df["desc"].apply(self.clean_text)

                years = sorted(df["fecha"].str[:4].unique())

                out_dir = os.path.join(API_OUTPUT_PATH, tipo)
                os.makedirs(out_dir, exist_ok=True)

                for yr in years:
                    sub = df[df["fecha"].str.startswith(yr)].copy()
                    cols = ["fecha", "cod", "label", "sector", "fob", "cif", "peso"]
                    sub[cols].to_json(os.path.join(out_dir, f"{yr}.json"), orient="records")

                    total = sub["cif"].sum() if tipo == "imports" else sub["fob"].sum()
                    resumen[tipo] = [x for x in resumen[tipo] if x["year"] != yr]
                    resumen[tipo].append({"year": yr, "total": round(float(total), 2), "file": f"{yr}.json"})

                processed += 1

            except Exception as e:
                status.error(f"❌ Error en {filename}: {e}")

        # escribir summary.json
        for t in resumen:
            if resumen[t]:
                resumen[t].sort(key=lambda x: x["year"], reverse=True)
                sum_path = os.path.join(API_OUTPUT_PATH, t, "summary.json")
                with open(sum_path, "w", encoding="utf-8") as f:
                    json.dump(resumen[t], f, ensure_ascii=False)

        status.success(f"✅ ETL completo: {processed} archivos procesados.")
        return processed > 0

    def git_push_changes(self):
        try:
            # ver estado
            run(["git", "status"], cwd=REPO_PATH)

            # add + commit (solo si hay cambios)
            run(["git", "add", "public/data"], cwd=REPO_PATH)

            # si no hay cambios staged, commit falla; lo manejamos
            diff = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=REPO_PATH,
                                  capture_output=True, text=True)
            if not diff.stdout.strip():
                return True, "No había cambios nuevos en public/data (nada que subir)."

            msg = f"Update trade APIs {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            run(["git", "commit", "-m", msg], cwd=REPO_PATH)

            # push
            run(["git", "push", "origin", DEFAULT_BRANCH], cwd=REPO_PATH)
            return True, "🚀 Cambios subidos. Pages (Actions) debería desplegar automáticamente."
        except Exception as e:
            return False, str(e)


# ==============================================================================
# UI
# ==============================================================================
st.sidebar.title("China Data Hub")
page = st.sidebar.radio("Menú", ["⚙️ Admin ETL", "📊 Dashboard (local)"])

etl = ETLEngine()

if page == "⚙️ Admin ETL":
    st.title("⚙️ ETL → JSON → GitHub Pages")

    st.caption("Flujo: procesas Excels en tu Mac → genera public/data → git push → GitHub Actions publica Pages.")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Ruta de Excels**")
        st.code(RAW_DATA_PATH)
    with c2:
        st.write("**Salida JSON**")
        st.code(API_OUTPUT_PATH)

    if st.button("🔄 Procesar Excels y generar JSON", type="primary"):
        with st.status("Ejecutando ETL...", expanded=True) as status:
            ok = etl.run_process(status)
        if ok:
            st.success("Listo. Ahora puedes subir a GitHub.")

    if st.button("☁️ Git commit + push (publicar)", type="secondary"):
        ok, msg = etl.git_push_changes()
        (st.success if ok else st.error)(msg)

elif page == "📊 Dashboard (local)":
    st.title("📊 Validación rápida (local)")

    flujo = st.selectbox("Flujo", ["imports", "exports"])
    summary_path = os.path.join(API_OUTPUT_PATH, flujo, "summary.json")

    if not os.path.exists(summary_path):
        st.info("No hay summary.json aún. Ve a Admin ETL y procesa.")
        st.stop()

    years = [x["year"] for x in json.load(open(summary_path, encoding="utf-8"))]
    year = st.selectbox("Año", years)

    data_path = os.path.join(API_OUTPUT_PATH, flujo, f"{year}.json")
    if not os.path.exists(data_path):
        st.warning("No existe el JSON del año seleccionado.")
        st.stop()

    df = pd.read_json(data_path)
    val = "cif" if flujo == "imports" else "fob"
    st.metric("Total USD", f"${df[val].sum():,.0f}")

    df["fecha"] = pd.to_datetime(df["fecha"])
    evol = df.groupby(df["fecha"].dt.to_period("M"))[val].sum().reset_index()
    evol["fecha"] = evol["fecha"].dt.to_timestamp()

    st.plotly_chart(px.area(evol, x="fecha", y=val, title="Evolución mensual"), use_container_width=True)

    top = df.groupby(["cod", "label"])[[val, "peso"]].sum().reset_index().sort_values(val, ascending=False).head(50)
    st.dataframe(top, hide_index=True, use_container_width=True)
