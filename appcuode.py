import streamlit as st
import pandas as pd
import json
import os
import re
import glob
import subprocess
import unicodedata
from datetime import datetime

# ==============================================================================
# CONFIG
# ==============================================================================
st.set_page_config(
    layout="wide",
    page_title="China Data Hub – CUODE ETL",
    page_icon="🇨🇳"
)

RAW_DATA_PATH = "/Users/paulvera/Desktop/China Data Hub/Cuode"
REPO_PATH = os.getcwd()
API_OUTPUT_PATH = os.path.join(REPO_PATH, "public", "data", "importscuode")
DEFAULT_BRANCH = "main"

# ==============================================================================
# HELPERS
# ==============================================================================
def norm(txt):
    if txt is None:
        return ""
    txt = str(txt).strip().lower()
    txt = ''.join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )
    txt = re.sub(r"\s+", " ", txt)
    return txt


def run(cmd, cwd=None):
    res = subprocess.run(
        cmd, cwd=cwd,
        capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(res.stderr or res.stdout)
    return res.stdout


# ==============================================================================
# ETL
# ==============================================================================
class CuodeETL:

    def clean_text(self, txt):
        if not isinstance(txt, str):
            return "Desconocido"
        txt = re.sub(r"\(.*?\)", "", txt)
        txt = txt.strip()
        return txt.capitalize() if txt else "Desconocido"

    def run_etl(self, status):
        status.write(f"📂 Leyendo CUODE desde: `{RAW_DATA_PATH}`")

        files = [
            f for f in glob.glob(os.path.join(RAW_DATA_PATH, "*.xlsx"))
            if not os.path.basename(f).startswith("~$")
        ]

        if not files:
            status.error("❌ No se encontraron archivos .xlsx válidos")
            return False

        os.makedirs(API_OUTPUT_PATH, exist_ok=True)

        summary = []
        processed = 0

        for filepath in sorted(files):
            fname = os.path.basename(filepath)
            status.write(f"🔄 Procesando {fname}")

            try:
                df = pd.read_excel(filepath, dtype=str, engine="openpyxl")
                df.columns = df.columns.astype(str).str.strip()

                # Normalizar columnas
                cols = {norm(c): c for c in df.columns}

                def pick(*opts):
                    for o in opts:
                        if o in cols:
                            return cols[o]
                    return None

                rename = {
                    pick("anio", "año"): "year",
                    pick("codigo cuode", "cuode"): "cuode",
                    pick("descripcion cuode", "descripcion"): "descripcion",
                    pick("valor cif", "cif"): "cif",
                    pick("peso neto", "peso"): "peso"
                }

                rename = {k: v for k, v in rename.items() if k}
                df = df.rename(columns=rename)

                if "year" not in df.columns or "cuode" not in df.columns:
                    status.warning(f"⚠️ Columnas clave faltantes en {fname}")
                    continue

                df["year"] = df["year"].astype(str).str[:4]
                df["cuode"] = df["cuode"].astype(str).str.strip()
                df["descripcion"] = df["descripcion"].apply(self.clean_text)

                for c in ["cif", "peso"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

                years = sorted(df["year"].unique())

                for yr in years:
                    sub = df[df["year"] == yr].copy()
                    out = os.path.join(API_OUTPUT_PATH, f"{yr}.json")
                    sub.to_json(out, orient="records", force_ascii=False)

                    summary = [x for x in summary if x["year"] != yr]
                    summary.append({
                        "year": yr,
                        "records": len(sub),
                        "total_cif": round(float(sub["cif"].sum()), 2),
                        "file": f"{yr}.json"
                    })

                processed += 1

            except Exception as e:
                status.error(f"❌ Error en {fname}: {e}")

        if summary:
            summary.sort(key=lambda x: x["year"], reverse=True)
            with open(os.path.join(API_OUTPUT_PATH, "summary.json"), "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)

        status.success(f"✅ ETL CUODE completo: {processed} archivos procesados")
        return processed > 0

    def git_publish(self):
        try:
            run(["git", "add", "public/data/importscuode"], cwd=REPO_PATH)

            diff = subprocess.run(
                ["git", "diff", "--cached", "--name-only"],
                cwd=REPO_PATH, capture_output=True, text=True
            )

            if not diff.stdout.strip():
                return True, "No había cambios nuevos que publicar."

            msg = f"Update CUODE APIs {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            run(["git", "commit", "-m", msg], cwd=REPO_PATH)
            run(["git", "push", "origin", DEFAULT_BRANCH], cwd=REPO_PATH)

            return True, "🚀 APIs CUODE publicadas en GitHub Pages"

        except Exception as e:
            return False, str(e)


# ==============================================================================
# UI
# ==============================================================================
etl = CuodeETL()

st.title("⚙️ ETL CUODE → APIs públicas")

st.caption(
    "Flujo: Excels CUODE → JSON → public/data/importscuode → git push → GitHub Pages"
)

st.code(RAW_DATA_PATH)
st.code(API_OUTPUT_PATH)

if st.button("🔄 Procesar Excels CUODE", type="primary"):
    with st.status("Ejecutando ETL CUODE...", expanded=True) as status:
        ok = etl.run_etl(status)
    if ok:
        st.success("CUODE procesado. Puedes publicar.")

if st.button("☁️ Publicar APIs CUODE (git push)", type="secondary"):
    ok, msg = etl.git_publish()
    (st.success if ok else st.error)(msg)
