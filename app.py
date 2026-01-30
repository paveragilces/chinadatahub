import streamlit as st
import pandas as pd
import json
import os
import re
import glob
import subprocess
import plotly.express as px

# ==============================================================================
# CONFIGURACIÓN DEL SISTEMA
# ==============================================================================
st.set_page_config(layout="wide", page_title="Observatorio Bilateral", page_icon="🇨🇳")

# ------------------------------------------------------------------------------
# RUTA DE TUS DATOS (Asegúrate que esta ruta sea correcta en tu Mac)
# ------------------------------------------------------------------------------
RAW_DATA_PATH = "/Users/paulvera/Desktop/China Data Hub/Subpartidas"

# Rutas automáticas del repositorio
REPO_PATH = os.getcwd()
API_OUTPUT_PATH = os.path.join(REPO_PATH, "public/data")

# ==============================================================================
# MOTOR ETL (VERSIÓN EXACTA PARA TUS ENCABEZADOS)
# ==============================================================================
class ETLEngine:
    def clean_text(self, text):
        """Limpia descripciones de productos para hacerlas legibles"""
        if not isinstance(text, str): return "Desconocido"
        txt = text.strip().upper()
        # Eliminar basura aduanera común
        patterns = [r"^LOS DEMÁS\s*", r"^LAS DEMÁS\s*", r"^OTRAS\s*", r"^OTROS\s*", r"\(.*?\)"]
        for p in patterns:
            txt = re.sub(p, "", txt)
        return txt.strip().capitalize() if len(txt) > 2 else text

    def get_sector(self, code):
        """Clasifica por capítulo (2 primeros dígitos)"""
        cap = str(code)[:2]
        sectors = {
            '03': '🦐 Pesca y Crustáceos', '08': '🍌 Banano y Frutas', '18': '🍫 Cacao',
            '44': '🪵 Madera', '16': '🥫 Conservas',
            '84': '⚙️ Maquinaria y Calderas', '85': '🔌 Tecnología/Eléctrico',
            '87': '🚗 Vehículos', '72': '🏗️ Hierro y Acero', '39': 'Plásticos',
            '29': '🧪 Químicos Orgánicos', '30': '💊 Farmacéuticos',
            '61': '👕 Textiles (Punto)', '62': '👔 Textiles (No Punto)', '64': '👞 Calzado'
        }
        return sectors.get(cap, '📦 Otros Sectores')

    def run_process(self, status_container):
        status_container.write(f"🚀 Iniciando búsqueda en: `{RAW_DATA_PATH}`")
        files = glob.glob(os.path.join(RAW_DATA_PATH, "*.xlsx"))
        
        if not files:
            status_container.error("❌ No encontré archivos .xlsx.")
            return False

        resumen_global = {'imports': [], 'exports': []}
        archivos_procesados = 0
        
        for i, filepath in enumerate(sorted(files)):
            filename = os.path.basename(filepath)
            status_container.write(f"🔄 Procesando: {filename}...")
            
            tipo = "exports" if "export" in filename.lower() else "imports"
            
            try:
                # 1. LEER EXCEL
                # Usamos skiprows=7 porque tus encabezados están en la fila 8
                df = pd.read_excel(filepath, skiprows=7, dtype=str)
                
                # --- BLINDAJE CRÍTICO ---
                # Convertimos todos los encabezados a string para evitar errores con celdas vacías (NaN)
                df.columns = df.columns.astype(str)
                # Eliminamos espacios en blanco alrededor de los nombres (ej: "FOB " -> "FOB")
                df.columns = df.columns.str.strip()
                
                # Eliminamos columnas "Unnamed" (columnas vacías de Excel)
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                
                # 2. MAPEO EXACTO (Respetando tildes y mayúsculas)
                mapeo_exacto = {
                    'Período': 'fecha_txt',
                    'Código Subpartida': 'cod',
                    'Subpartida': 'desc',
                    'TM (Peso Neto)': 'peso',
                    'FOB': 'fob',
                    'CIF': 'cif'
                }
                
                # Renombrar solo las columnas que encuentre
                cols_found = {k:v for k,v in mapeo_exacto.items() if k in df.columns}
                df.rename(columns=cols_found, inplace=True)
                
                # Validación: Si no encontró la columna código, algo anda mal con ese archivo
                if 'cod' not in df.columns: 
                    status_container.warning(f"⚠️ {filename}: No encontré la columna 'Código Subpartida'. Columnas halladas: {list(df.columns)}")
                    continue

                # 3. LIMPIEZA DE DATOS
                # Regex para fecha formato "2024 / 01 - Ene"
                def parse_fecha(txt):
                    match = re.search(r'(\d{4})\s*/\s*(\d{2})', str(txt))
                    if match:
                        return f"{match.group(1)}-{match.group(2)}-01"
                    return None

                df['fecha'] = df['fecha_txt'].apply(parse_fecha)
                df = df.dropna(subset=['fecha', 'cod'])
                
                # Convertir números
                for c in ['fob', 'cif', 'peso']:
                    if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                
                # Estandarizar
                df['cod'] = df['cod'].str.replace('.', '', regex=False).str.strip().str.zfill(10)
                df['sector'] = df['cod'].apply(self.get_sector)
                df['label'] = df['desc'].apply(self.clean_text)

                # 4. GUARDADO
                years = df['fecha'].str[:4].unique()
                for yr in years:
                    sub_df = df[df['fecha'].str.startswith(yr)]
                    out_dir = os.path.join(API_OUTPUT_PATH, tipo)
                    os.makedirs(out_dir, exist_ok=True)
                    
                    # JSON Data
                    cols = ['fecha', 'cod', 'label', 'sector', 'fob', 'cif', 'peso']
                    valid_cols = [c for c in cols if c in sub_df.columns]
                    sub_df[valid_cols].to_json(os.path.join(out_dir, f"{yr}.json"), orient='records')
                    
                    # KPI Resumen
                    val = sub_df['fob'].sum() if tipo == 'exports' else sub_df['cif'].sum()
                    resumen_global[tipo] = [x for x in resumen_global[tipo] if x['year'] != yr]
                    resumen_global[tipo].append({"year": yr, "total": round(val, 2), "file": f"{yr}.json"})
                
                archivos_procesados += 1
            
            except Exception as e:
                status_container.error(f"❌ Error en {filename}: {str(e)}")

        # 5. GENERAR ÍNDICES
        if archivos_procesados > 0:
            for t, data in resumen_global.items():
                if data:
                    data.sort(key=lambda x: x['year'], reverse=True)
                    os.makedirs(os.path.join(API_OUTPUT_PATH, t), exist_ok=True)
                    with open(os.path.join(API_OUTPUT_PATH, t, "summary.json"), 'w') as f:
                        json.dump(data, f)
            status_container.success(f"✅ ¡Listo! {archivos_procesados} archivos procesados.")
            return True
        return False

    def git_push_changes(self):
        try:
            subprocess.run(["git", "add", "public/data"], check=True, cwd=REPO_PATH)
            subprocess.run(["git", "commit", "-m", "Data Update"], check=True, cwd=REPO_PATH)
            subprocess.run(["git", "push"], check=True, cwd=REPO_PATH)
            return True, "Cambios subidos a GitHub."
        except Exception as e:
            return False, str(e)

# ==============================================================================
# UI STREAMLIT
# ==============================================================================
page = st.sidebar.radio("Menú", ["📊 Dashboard", "⚙️ Admin ETL"])

if page == "📊 Dashboard":
    st.title("🇨🇳 Observatorio Bilateral EC-CN")
    
    col1, col2 = st.columns(2)
    flujo = col1.selectbox("Flujo", ["imports", "exports"])
    
    summary_path = os.path.join(API_OUTPUT_PATH, flujo, "summary.json")
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            years = [x['year'] for x in json.load(f)]
        year = col2.selectbox("Año", years)
        
        data_path = os.path.join(API_OUTPUT_PATH, flujo, f"{year}.json")
        if os.path.exists(data_path):
            df = pd.read_json(data_path)
            
            # KPIs
            val_col = 'cif' if flujo == 'imports' else 'fob'
            st.metric("Total USD", f"${df[val_col].sum():,.0f}")
            
            # Gráficos
            tab1, tab2 = st.tabs(["Gráficos", "Tabla Detalle"])
            with tab1:
                df['fecha'] = pd.to_datetime(df['fecha'])
                # Evolución
                evol = df.groupby(df['fecha'].dt.to_period('M'))[val_col].sum().reset_index()
                evol['fecha'] = evol['fecha'].dt.to_timestamp()
                st.plotly_chart(px.area(evol, x='fecha', y=val_col, title="Evolución Mensual"), use_container_width=True)
                
                # Sectores
                sect = df.groupby('sector')[val_col].sum().reset_index().sort_values(val_col, ascending=False).head(10)
                st.plotly_chart(px.pie(sect, values=val_col, names='sector', title="Top Sectores"), use_container_width=True)
            
            with tab2:
                top = df.groupby(['cod', 'label'])[[val_col, 'peso']].sum().reset_index().sort_values(val_col, ascending=False).head(50)
                st.dataframe(top, hide_index=True, use_container_width=True)
    else:
        st.info("⚠️ No hay datos procesados. Ve a la pestaña 'Admin ETL'.")

elif page == "⚙️ Admin ETL":
    st.title("⚙️ Procesamiento de Datos")
    if st.button("🔄 Procesar Excels y Generar APIs", type="primary"):
        etl = ETLEngine()
        with st.status("Ejecutando...", expanded=True) as status:
            if etl.run_process(status):
                st.balloons()
    
    if st.button("☁️ Subir a GitHub"):
        etl = ETLEngine()
        ok, msg = etl.git_push_changes()
        if ok: st.success(msg)
        else: st.error(msg)