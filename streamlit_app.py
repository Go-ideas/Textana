#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import hmac
import hashlib
import os
import re
import subprocess
import sys
import time
import unicodedata
import zipfile
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
from wordcloud import STOPWORDS, WordCloud
import base64

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:
    Fernet = None
    InvalidToken = Exception


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"

STEP_LABELS = {
    "paso1_lectura.py": "Paso 1 - Lectura",
    "paso2_mejora.py": "Paso 2 - Mejora",
    "paso3_clasificacion.py": "Paso 3 - Clasificacion",
    "paso4_export_spss.py": "Paso 4 - Exportacion SPSS",
    "paso5_graficos.py": "Paso 5 - Graficos",
}


def clean_text_value(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return ""
    return s


def format_treemap_label(text: str, max_chars_line: int, max_lines: int) -> str:
    words = clean_text_value(text).split()
    if not words:
        return ""
    lines: list[str] = []
    current = ""
    for w in words:
        nxt = w if not current else f"{current} {w}"
        if len(nxt) <= max_chars_line:
            current = nxt
            continue
        if current:
            lines.append(current)
            current = w
        else:
            lines.append(w[:max_chars_line])
            current = w[max_chars_line:]
        if len(lines) >= max_lines:
            break
    if len(lines) < max_lines and current:
        lines.append(current)

    lines = lines[:max_lines]
    original = clean_text_value(text)
    built = " ".join(lines).replace("<br>", " ")
    if len(built) < len(original) and lines:
        lines[-1] = lines[-1].rstrip(". ") + "..."
    return "<br>".join(lines)


def shorten_label(text: str, max_len: int) -> str:
    s = clean_text_value(text)
    if len(s) <= max_len:
        return s
    return s[: max_len - 3].rstrip() + "..."


def smart_truncate_label(text: str, max_len: int) -> str:
    s = clean_text_value(text)
    if len(s) <= max_len:
        return s
    if max_len <= 4:
        return s[:max_len]
    head = s[: max_len - 3]
    # Intenta cortar por palabra para no truncar "feo".
    if " " in head:
        head2 = head.rsplit(" ", 1)[0].strip()
        if len(head2) >= max(4, max_len // 2):
            head = head2
    return head.rstrip(" ,.;:-") + "..."


def normalize_label_text(text: str) -> str:
    s = clean_text_value(text)
    return re.sub(r"\s+", " ", s).strip()


def similarity_key(text: str) -> str:
    s = normalize_label_text(text).lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_similar_label_map(labels: list[str], threshold: float) -> dict[str, str]:
    cleaned = [normalize_label_text(x) for x in labels if normalize_label_text(x)]
    if not cleaned:
        return {}
    freq = Counter(cleaned)
    ordered = sorted(freq.keys(), key=lambda x: (-freq[x], -len(x), x))
    reps: list[str] = []
    rep_keys: list[str] = []
    out: dict[str, str] = {}
    # Agrupacion suave por similitud textual para reducir ruido de etiquetas casi iguales.
    for lab in ordered:
        k = similarity_key(lab)
        assigned = None
        for rep, rep_k in zip(reps, rep_keys):
            if SequenceMatcher(None, k, rep_k).ratio() >= threshold:
                assigned = rep
                break
        if assigned is None:
            reps.append(lab)
            rep_keys.append(k)
            assigned = lab
        out[lab] = assigned
    return out


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    if len(h) != 6:
        return f"rgba(80,80,80,{alpha})"
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha:.3f})"


def get_shared_report_theme() -> dict[str, object]:
    # Sistema visual compartido para alinear Sankey y Treemap en el entregable.
    return {
        "group_palette": [
            "#1F4E79",
            "#2F6B92",
            "#4E7D6B",
            "#7A6F9B",
            "#8A6D5A",
            "#3D5A80",
            "#5A7D5A",
            "#7F6A93",
            "#4B6A88",
            "#6C7A89",
        ],
        "treemap_scale": [
            [0.00, "#E8EFF6"],
            [0.25, "#BFD0E3"],
            [0.50, "#8AAECC"],
            [0.75, "#4E86B5"],
            [1.00, "#1F4E79"],
        ],
        "paper_bg": "#F7F9FC",
        "plot_bg": "#F7F9FC",
        "font_color": "#1F2D3D",
        "border_color": "rgba(60, 78, 96, 0.32)",
    }


def fig_to_png_bytes(fig) -> bytes | None:
    try:
        return fig.to_image(format="png", scale=2)
    except Exception:
        return None


def build_zip_bytes(assets: dict[str, bytes]) -> bytes:
    mem = BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in assets.items():
            zf.writestr(name, data)
    mem.seek(0)
    return mem.read()


def get_textana_signing_key() -> bytes:
    # Clave local para firmar/validar paquetes .textana.
    env_key = os.getenv("TEXTANA_SIGN_KEY", "").strip()
    if env_key:
        return env_key.encode("utf-8")
    key_path = ROOT / ".textana_signing_key"
    if key_path.exists():
        return key_path.read_bytes()
    key = os.urandom(32)
    key_path.write_bytes(key)
    return key


def secure_package_available() -> bool:
    return Fernet is not None


def get_textana_fernet_key() -> bytes:
    # Deriva una clave Fernet estable desde la clave local maestra.
    raw_master = get_textana_signing_key()
    digest = hashlib.sha256(raw_master).digest()
    return base64.urlsafe_b64encode(digest)


def build_secure_textana_package(excel_bytes: bytes, source_name: str, extra_files: dict[str, bytes] | None = None) -> bytes:
    if not secure_package_available():
        raise RuntimeError("No esta disponible cryptography/Fernet para cifrar paquetes .textana.")

    extra_files = extra_files or {}
    extra_files_b64 = {str(k): base64.b64encode(v).decode("ascii") for k, v in extra_files.items()}
    payload = {
        "format": "textana-secure-package",
        "version": 2,
        "source_name": source_name,
        "payload_sha256": hashlib.sha256(excel_bytes).hexdigest(),
        "payload_b64": base64.b64encode(excel_bytes).decode("ascii"),
        "extra_files_b64": extra_files_b64,
    }
    payload_bytes = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    token = Fernet(get_textana_fernet_key()).encrypt(payload_bytes)
    # Formato binario propio: no es ZIP, evita deteccion trivial por extension/cabecera.
    return b"TEXTANA2\n" + token


def validate_secure_textana_package(pkg_bytes: bytes) -> tuple[bool, str, bytes | None, str | None, dict[str, bytes]]:
    # Formato actual cifrado (v2).
    try:
        if pkg_bytes.startswith(b"TEXTANA2\n"):
            if not secure_package_available():
                return False, "No se puede validar: falta cryptography/Fernet en este entorno.", None, None, {}
            token = pkg_bytes.split(b"\n", 1)[1]
            decoded = Fernet(get_textana_fernet_key()).decrypt(token)
            data = json.loads(decoded.decode("utf-8"))
            if data.get("format") != "textana-secure-package" or int(data.get("version", 0)) != 2:
                return False, "Paquete cifrado invalido: metadatos no compatibles.", None, None, {}
            payload = base64.b64decode(str(data.get("payload_b64", "")).encode("ascii"), validate=True)
            payload_hash = hashlib.sha256(payload).hexdigest()
            if payload_hash != str(data.get("payload_sha256", "")):
                return False, "Paquete cifrado invalido: hash de contenido no coincide.", None, None, {}
            extras: dict[str, bytes] = {}
            for rel_path, b64val in dict(data.get("extra_files_b64", {})).items():
                try:
                    rp = str(rel_path).replace("\\", "/").lstrip("/")
                    if ".." in rp.split("/"):
                        continue
                    extras[rp] = base64.b64decode(str(b64val).encode("ascii"), validate=True)
                except Exception:
                    continue
            src_name = str(data.get("source_name", "resultado.xlsx"))
            return True, "Paquete cifrado validado correctamente.", payload, src_name, extras
    except InvalidToken:
        return False, "No se pudo descifrar el paquete: clave incorrecta o archivo alterado.", None, None, {}
    except Exception:
        # Si no es v2, intenta compatibilidad con formato legado v1 (ZIP firmado).
        pass

    try:
        with zipfile.ZipFile(BytesIO(pkg_bytes), mode="r") as zf:
            names = set(zf.namelist())
            required = {"payload.xlsx", "manifest.json", "signature.txt"}
            if not required.issubset(names):
                return False, "Paquete invalido: faltan archivos requeridos.", None, None, {}

            payload = zf.read("payload.xlsx")
            manifest_raw = zf.read("manifest.json")
            sig_raw = zf.read("signature.txt").decode("utf-8").strip()
            manifest = json.loads(manifest_raw.decode("utf-8"))

            if manifest.get("format") != "textana-secure-package":
                return False, "Paquete invalido: formato no reconocido.", None, None, {}
            if int(manifest.get("version", 0)) != 1:
                return False, "Paquete invalido: version no compatible.", None, None, {}
            if manifest.get("payload_name") != "payload.xlsx":
                return False, "Paquete invalido: payload no compatible.", None, None, {}

            payload_hash = hashlib.sha256(payload).hexdigest()
            if payload_hash != str(manifest.get("payload_sha256", "")):
                return False, "Paquete invalido: hash de contenido no coincide.", None, None, {}

            sign_input = manifest_raw + b"\n" + payload
            expected_sig = hmac.new(get_textana_signing_key(), sign_input, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig_raw, expected_sig):
                return False, "Firma invalida: archivo alterado o no emitido por esta instancia.", None, None, {}

            src_name = str(manifest.get("source_name", "resultado.xlsx"))
            return True, "Paquete validado correctamente.", payload, src_name, {}
    except Exception as exc:
        return False, f"No se pudo leer el paquete seguro: {exc}", None, None, {}


def apply_layout_tweaks() -> None:
    st.markdown(
        """
        <style>
          .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 96rem;
          }
          [data-testid="stHorizontalBlock"] {
            gap: 0.75rem;
          }
          .stPlotlyChart {
            border: 1px solid #e6e6e6;
            border-radius: 10px;
            padding: 0.25rem;
          }
          /* Evita contornos/sombras en texto de Plotly (incluye Sankey). */
          .js-plotly-plot .plotly text {
            text-shadow: none !important;
            -webkit-text-stroke: 0 !important;
            stroke: none !important;
            paint-order: fill !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def save_upload(uploaded_file) -> Path:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    target = INPUT_DIR / uploaded_file.name
    target.write_bytes(uploaded_file.getbuffer())
    return target


def save_uploaded_result_excel(uploaded_bytes: bytes, stem: str = "Paso3_clasificacion_cargado") -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", str(stem)).strip("._") or "resultado_cargado"
    # Nombre unico para evitar bloqueos por archivo abierto en Excel.
    target = OUTPUT_DIR / f"{safe_stem}_{time.time_ns()}.xlsx"
    target.write_bytes(uploaded_bytes)
    return target


def get_excel_metadata(excel_path: Path) -> tuple[list[str], dict[str, list[str]]]:
    xls = pd.ExcelFile(excel_path, engine="openpyxl")
    sheets = xls.sheet_names
    columns_map: dict[str, list[str]] = {}

    for sheet in sheets:
        preview = pd.read_excel(excel_path, sheet_name=sheet, nrows=0, engine="openpyxl")
        columns_map[sheet] = [str(c).strip() for c in preview.columns]

    return sheets, columns_map


def check_viewer_excel_completeness(excel_path: Path) -> tuple[bool, list[str]]:
    issues: list[str] = []
    try:
        xls = pd.ExcelFile(excel_path, engine="openpyxl")
        sheets = set(xls.sheet_names)
    except Exception as exc:
        return False, [f"No se pudo abrir el Excel: {exc}"]

    if "LDC" not in sheets:
        issues.append("Falta hoja `LDC`.")
    if "clasificacion_sin_incorrectos" not in sheets:
        issues.append("Falta hoja `clasificacion_sin_incorrectos`.")

    if "LDC" in sheets:
        try:
            ldc_cols = pd.read_excel(excel_path, sheet_name="LDC", nrows=0, engine="openpyxl").columns.astype(str).tolist()
            if "Frecuencia" not in ldc_cols:
                issues.append("En `LDC` falta columna `Frecuencia`.")
        except Exception:
            issues.append("No se pudieron leer columnas de `LDC`.")

    if "clasificacion_sin_incorrectos" in sheets:
        try:
            corr_cols = pd.read_excel(excel_path, sheet_name="clasificacion_sin_incorrectos", nrows=0, engine="openpyxl").columns.astype(str).tolist()
            top_cols = [c for c in corr_cols if str(c).startswith("Topico_")]
            if len(top_cols) == 0:
                issues.append("En `clasificacion_sin_incorrectos` faltan columnas `Topico_*`.")
            elif len(top_cols) < 2:
                issues.append("Solo hay una columna `Topico_*`; Sankey/Heatmap pueden verse limitados.")
        except Exception:
            issues.append("No se pudieron leer columnas de `clasificacion_sin_incorrectos`.")

    return len(issues) == 0, issues


def collect_viewer_extra_files() -> dict[str, bytes]:
    extras: dict[str, bytes] = {}
    graficos_dir = OUTPUT_DIR / "graficos"
    if graficos_dir.exists():
        for fp in graficos_dir.rglob("*"):
            if not fp.is_file():
                continue
            rel = fp.relative_to(OUTPUT_DIR).as_posix()
            try:
                extras[rel] = fp.read_bytes()
            except Exception:
                continue
    # Incluye imagenes base por compatibilidad con viewer liviano.
    for name in ["barras_topicos.png", "pareto_topicos.png", "treemap_topicos.png"]:
        fp = OUTPUT_DIR / name
        if fp.exists() and fp.is_file():
            try:
                extras[name] = fp.read_bytes()
            except Exception:
                continue
    return extras


def restore_viewer_extra_files(extra_files: dict[str, bytes]) -> None:
    for rel_path, content in extra_files.items():
        rel_norm = str(rel_path).replace("\\", "/").lstrip("/")
        if ".." in rel_norm.split("/"):
            continue
        target = OUTPUT_DIR / rel_norm
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def render_copy_button(log_text: str) -> None:
    payload = json.dumps(log_text)
    components.html(
        f"""
        <button id="copy-log-btn" style="padding:6px 10px;border:1px solid #bbb;border-radius:6px;background:#f6f6f6;cursor:pointer;">
          Copiar log
        </button>
        <span id="copy-log-msg" style="margin-left:10px;font-size:12px;color:#1f7a1f;"></span>
        <script>
          const LOG_TEXT = {payload};
          const btn = document.getElementById('copy-log-btn');
          const msg = document.getElementById('copy-log-msg');
          btn.onclick = async () => {{
            try {{
              await navigator.clipboard.writeText(LOG_TEXT);
              msg.textContent = 'Log copiado al portapapeles';
            }} catch (e) {{
              msg.textContent = 'No se pudo copiar automaticamente';
            }}
          }};
        </script>
        """,
        height=50,
    )


def init_step_report(export_spss: bool) -> tuple[list[str], dict[str, dict]]:
    planned = ["paso1_lectura.py", "paso2_mejora.py", "paso3_clasificacion.py", "paso5_graficos.py"]
    if export_spss:
        planned.insert(3, "paso4_export_spss.py")

    report = {
        step: {
            "step": STEP_LABELS[step],
            "status": "pendiente",
            "start": None,
            "end": None,
            "duration_s": None,
        }
        for step in planned
    }
    return planned, report


def calculate_eta(report: dict[str, dict], running_step: str | None) -> str:
    completed = [r["duration_s"] for r in report.values() if r["status"] in {"completado", "omitido"} and r["duration_s"] is not None]
    pending_count = sum(1 for r in report.values() if r["status"] == "pendiente")

    if running_step:
        running = report[running_step]
        if running["start"] is not None:
            pending_count += 1

    if not completed:
        return "ETA: calculando..."

    avg = sum(completed) / len(completed)
    remaining = avg * pending_count
    return f"ETA aprox: {remaining/60:.1f} min"


def report_to_dataframe(report: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for r in report.values():
        dur = r["duration_s"]
        rows.append(
            {
                "Paso": r["step"],
                "Estado": r["status"],
                "Duracion (s)": round(dur, 2) if dur is not None else None,
            }
        )
    return pd.DataFrame(rows)


def parse_progress_line(line: str) -> dict[str, int | str] | None:
    s = line.strip()
    if not s.startswith("[PROGRESS]"):
        return None
    payload = s.replace("[PROGRESS]", "", 1).strip()
    parts: dict[str, str] = {}
    for token in payload.split():
        if "=" in token:
            k, v = token.split("=", 1)
            parts[k.strip()] = v.strip()
    if "step" not in parts:
        return None
    out: dict[str, int | str] = {"step": parts["step"]}
    for k in ("done", "total", "remaining"):
        if k in parts:
            try:
                out[k] = int(parts[k])
            except Exception:
                pass
    return out


@st.cache_data(show_spinner=False)
def load_pipeline_outputs(result_excel: str, file_version: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    _ = file_version  # invalida cache cuando cambia el archivo
    path = Path(result_excel)
    df_ldc = pd.read_excel(path, sheet_name="LDC", engine="openpyxl")
    df_corr = pd.read_excel(path, sheet_name="clasificacion_sin_incorrectos", engine="openpyxl")
    return df_ldc, df_corr


def render_visual_dashboard(
    result_excel: Path,
    key_prefix: str = "viz",
    selected_cross_vars_param: list[str] | None = None,
    show_cross_section: bool = True,
) -> None:
    if not result_excel.exists():
        st.info("Aun no existe archivo de resultados para visualizar.")
        return

    try:
        file_version = result_excel.stat().st_mtime_ns
        df_ldc, df_corr = load_pipeline_outputs(str(result_excel), file_version)
    except Exception as exc:
        st.error(f"No se pudieron cargar los resultados para visualizacion: {exc}")
        return

    if "Frecuencia" not in df_ldc.columns:
        st.warning("La hoja LDC no contiene la columna esperada: Frecuencia.")
        return

    df_ldc = df_ldc.copy()
    if "Contexto" in df_ldc.columns and df_ldc["Contexto"].apply(clean_text_value).ne("").any():
        item_col = "Contexto"
        item_label = "Contexto"
    elif "Topico" in df_ldc.columns:
        item_col = "Topico"
        item_label = "Topico"
    else:
        st.warning("La hoja LDC no contiene columnas para etiquetar graficos (Contexto/Topico).")
        return

    df_ldc["Item"] = df_ldc[item_col].apply(clean_text_value)
    df_ldc = df_ldc[df_ldc["Item"] != ""]

    st.subheader("Visualizador interactivo")
    export_assets: dict[str, bytes] = {}

    c1, c2, c3 = st.columns(3)
    with c1:
        top_n = st.slider(f"Top N {item_label.lower()}s", min_value=3, max_value=min(50, max(3, len(df_ldc))), value=min(15, max(3, len(df_ldc))))
    with c2:
        min_freq = st.number_input("Frecuencia minima", min_value=1, value=1)
    with c3:
        palette = st.selectbox("Paleta", options=["Blues", "Viridis", "Cividis", "Plasma", "Magma", "Turbo"], index=0)

    has_network = (OUTPUT_DIR / "graficos" / "red_interactiva.html").exists()
    graph_options = ["Barras", "Pareto", "Treemap", "Nube de palabras", "Heatmap", "Sankey"]
    if has_network:
        graph_options.append("Red interactiva")
    viz_types = graph_options
    st.caption("Visualizacion completa: se muestran todos los graficos disponibles.")

    df_plot = df_ldc.copy()
    df_plot = df_plot[df_plot["Frecuencia"] >= min_freq]
    # Consolidar por contexto para evitar duplicados al venir Topico+Contexto en LDC.
    df_plot = (
        df_plot.groupby("Item", as_index=False)["Frecuencia"]
        .sum()
        .sort_values("Frecuencia", ascending=False)
        .head(top_n)
    )
    total_plot = df_plot["Frecuencia"].sum()
    if total_plot > 0:
        df_plot["Porcentaje"] = df_plot["Frecuencia"] / total_plot * 100
    else:
        df_plot["Porcentaje"] = 0.0

    if df_plot.empty:
        st.warning("No hay datos para graficar con esos filtros.")

    if "Barras" in viz_types and not df_plot.empty:
        with st.expander("Editar: Barras", expanded=True):
            bc1, bc2, bc3 = st.columns(3)
            with bc1:
                orient = st.selectbox("Orientacion", options=["Horizontal", "Vertical"], index=0, key=f"{key_prefix}_bar_orient")
            with bc2:
                bar_height = st.slider("Alto (px)", min_value=300, max_value=1000, value=520, step=10, key=f"{key_prefix}_bar_height")
            with bc3:
                show_values = st.checkbox("Mostrar valores", value=True, key=f"{key_prefix}_bar_values")

        if orient == "Horizontal":
            fig_bar = px.bar(
                df_plot.sort_values("Frecuencia", ascending=True),
                x="Frecuencia",
                y="Item",
                color="Frecuencia",
                color_continuous_scale=palette,
                title=f"Frecuencia por {item_label.lower()}",
            )
        else:
            fig_bar = px.bar(
                df_plot,
                x="Item",
                y="Frecuencia",
                color="Frecuencia",
                color_continuous_scale=palette,
                title=f"Frecuencia por {item_label.lower()}",
            )
            fig_bar.update_xaxes(tickangle=45)
        fig_bar.update_layout(height=bar_height)
        fig_bar.update_traces(
            customdata=df_plot[["Porcentaje"]].values if orient == "Vertical" else df_plot.sort_values("Frecuencia", ascending=True)[["Porcentaje"]].values,
            hovertemplate="%{y}<br>Frecuencia: %{x}<br>Porcentaje: %{customdata[0]:.0f}%<extra></extra>" if orient == "Horizontal" else "%{x}<br>Frecuencia: %{y}<br>Porcentaje: %{customdata[0]:.0f}%<extra></extra>",
        )
        if show_values:
            fig_bar.update_traces(
                textposition="outside",
                texttemplate="%{customdata[0]:.0f}%",
                cliponaxis=False,
            )
        st.plotly_chart(fig_bar, use_container_width=True)
        bar_png = fig_to_png_bytes(fig_bar)
        if bar_png:
            export_assets["editado/01_barras.png"] = bar_png

    if "Pareto" in viz_types and not df_plot.empty:
        try:
            with st.expander("Editar: Pareto", expanded=False):
                pc1, pc2, pc3 = st.columns(3)
                with pc1:
                    pareto_height = st.slider("Alto Pareto (px)", min_value=300, max_value=1000, value=520, step=10, key=f"{key_prefix}_par_height")
                with pc2:
                    pareto_target = st.slider("Linea objetivo (%)", min_value=50, max_value=99, value=80, step=1, key=f"{key_prefix}_par_target")
                with pc3:
                    show_markers = st.checkbox("Marcadores linea", value=True, key=f"{key_prefix}_par_markers")

            pareto_df = df_plot.sort_values("Frecuencia", ascending=False).copy()
            pareto_df["Porcentaje"] = pareto_df["Frecuencia"] / pareto_df["Frecuencia"].sum() * 100
            pareto_df["AcumuladoPct"] = pareto_df["Porcentaje"].cumsum()
            fig_pareto = px.bar(
                pareto_df,
                x="Item",
                y="Porcentaje",
                title=f"Pareto de {item_label.lower()}s",
            )
            fig_pareto.update_xaxes(tickangle=45)
            fig_pareto.update_traces(
                texttemplate="%{y:.0f}%",
                textposition="outside",
                hovertemplate="%{x}<br>% individual: %{y:.0f}%<extra></extra>",
            )
            fig_pareto.add_scatter(
                x=pareto_df["Item"],
                y=pareto_df["AcumuladoPct"],
                mode="lines+markers" if show_markers else "lines",
                yaxis="y2",
                name="% acumulado",
                hovertemplate="%{x}<br>% acumulado: %{y:.0f}%<extra></extra>",
            )
            fig_pareto.add_hline(y=pareto_target, line_dash="dash", line_color="gray", yref="y2")

            # Marca la interseccion del acumulado con la linea objetivo.
            inter_idx = pareto_df[pareto_df["AcumuladoPct"] >= pareto_target].index
            if len(inter_idx) > 0:
                i0 = int(inter_idx[0])
                ix = pareto_df.loc[i0, "Item"]
                iy = float(pareto_df.loc[i0, "AcumuladoPct"])
                fig_pareto.add_scatter(
                    x=[ix],
                    y=[iy],
                    yaxis="y2",
                    mode="markers+text",
                    marker=dict(size=12, color="crimson", symbol="diamond"),
                    text=[f"Interseccion {iy:.0f}%"],
                    textposition="top center",
                    name="Interseccion",
                    hovertemplate=f"{ix}<br>Interseccion: {iy:.0f}%<extra></extra>",
                )

            fig_pareto.update_layout(
                height=pareto_height,
                yaxis=dict(title="% individual", tickformat=".0f"),
                yaxis2=dict(
                    title="% acumulado",
                    overlaying="y",
                    side="right",
                    range=[0, 100],
                    tickformat=".0f",
                )
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
            pareto_png = fig_to_png_bytes(fig_pareto)
            if pareto_png:
                export_assets["editado/02_pareto.png"] = pareto_png
        except Exception as exc:
            st.error(f"No se pudo renderizar Pareto: {exc}")

    if "Treemap" in viz_types and not df_plot.empty:
        report_theme = get_shared_report_theme()
        with st.expander("Editar: Treemap", expanded=False):
            tm1, tm2, tm3, tm4 = st.columns(4)
            with tm1:
                tree_text_mode = st.selectbox(
                    "Texto en cajas",
                    options=[
                        "Etiqueta",
                        "Etiqueta + valor",
                        "Etiqueta + % padre",
                        "Etiqueta + % total",
                        "Completo",
                    ],
                    index=0,
                    key=f"{key_prefix}_tree_text_mode",
                )
            with tm2:
                tree_font_size = st.slider("Tamano fuente", min_value=10, max_value=36, value=14, step=1, key=f"{key_prefix}_tree_font")
            with tm3:
                tree_max_depth = st.slider("Profundidad", min_value=1, max_value=6, value=2, step=1, key=f"{key_prefix}_tree_depth")
            with tm4:
                tree_height = st.slider("Alto treemap (px)", min_value=350, max_value=1000, value=620, step=10, key=f"{key_prefix}_tree_height")

            tm5, tm6, tm7, tm8 = st.columns(4)
            with tm5:
                tree_pad = st.slider("Separacion cajas", min_value=0, max_value=8, value=1, step=1, key=f"{key_prefix}_tree_pad")
            with tm6:
                tree_sort = st.checkbox("Ordenar por frecuencia", value=True, key=f"{key_prefix}_tree_sort")
            with tm7:
                tree_chars_line = st.slider("Chars por linea", min_value=8, max_value=40, value=18, step=1, key=f"{key_prefix}_tree_chars")
            with tm8:
                tree_max_lines = st.slider("Lineas max", min_value=1, max_value=5, value=2, step=1, key=f"{key_prefix}_tree_lines")

            tree_auto_font = st.checkbox("Fuente auto-ajustada", value=True, key=f"{key_prefix}_tree_auto_font")
            tree_palette_name = st.selectbox(
                "Paleta de colores",
                options=[
                    "Pastel elegante",
                    "Sankey sobria",
                    "Blues suave",
                    "Verde salvia",
                    "Gris editorial",
                ],
                index=0,
                key=f"{key_prefix}_tree_palette",
            )

        text_map = {
            "Etiqueta": "label",
            "Etiqueta + valor": "label+value",
            "Etiqueta + % padre": "label+percent parent",
            "Etiqueta + % total": "label+percent root",
            "Completo": "label+value+percent parent+percent root",
        }

        tree_df = df_plot.copy()
        if tree_sort:
            tree_df = tree_df.sort_values("Frecuencia", ascending=False)
        tree_df["Topico_Display"] = tree_df["Item"].astype(str).apply(
            lambda x: format_treemap_label(x, tree_chars_line, tree_max_lines)
        )
        treemap_palettes = {
            "Pastel elegante": [
                [0.00, "#E8E3DA"],
                [0.35, "#D5DDDA"],
                [0.65, "#B7C9D7"],
                [1.00, "#8FAEC7"],
            ],
            "Sankey sobria": [
                [0.00, "#DDE6EF"],
                [0.35, "#B7C9DB"],
                [0.65, "#6E8FAE"],
                [1.00, "#1F4E79"],
            ],
            "Blues suave": [
                [0.00, "#E9F0F7"],
                [0.35, "#C7D7E8"],
                [0.65, "#9FBAD3"],
                [1.00, "#6D94B8"],
            ],
            "Verde salvia": [
                [0.00, "#EEF2EA"],
                [0.35, "#D4E0CF"],
                [0.65, "#ADC5A5"],
                [1.00, "#7FA07B"],
            ],
            "Gris editorial": [
                [0.00, "#EEF1F3"],
                [0.35, "#D8DEE3"],
                [0.65, "#B8C3CC"],
                [1.00, "#8795A3"],
            ],
        }
        selected_scale = treemap_palettes.get(tree_palette_name, treemap_palettes["Pastel elegante"])

        fig_tree = px.treemap(
            tree_df,
            path=["Topico_Display"],
            values="Frecuencia",
            color="Frecuencia",
            color_continuous_scale=selected_scale,
            title=f"Treemap de {item_label.lower()}s",
            custom_data=["Item"],
        )
        fig_tree.update_traces(
            textinfo=text_map[tree_text_mode],
            textfont={"size": tree_font_size},
            maxdepth=tree_max_depth,
            marker={
                "pad": {"t": tree_pad, "r": tree_pad, "b": tree_pad, "l": tree_pad},
                "line": {"width": 1, "color": report_theme["border_color"]},
            },
            hovertemplate=f"{item_label}: %{{customdata[0]}}<br>Frecuencia: %{{value}}<br>% total: %{{percentRoot:.1%}}<extra></extra>",
        )
        if tree_auto_font:
            fig_tree.update_layout(uniformtext=dict(minsize=9, mode="hide"))
        fig_tree.update_layout(
            height=tree_height,
            margin={"l": 8, "r": 8, "t": 48, "b": 8},
            paper_bgcolor=report_theme["paper_bg"],
            plot_bgcolor=report_theme["plot_bg"],
            font={"color": report_theme["font_color"], "family": "Segoe UI"},
            showlegend=False,
            coloraxis_showscale=True,
            coloraxis_colorbar=dict(
                tickfont={"color": report_theme["font_color"]},
                title=dict(text="Frecuencia", font={"color": report_theme["font_color"]}),
            ),
        )
        st.plotly_chart(fig_tree, use_container_width=True)
        tree_png = fig_to_png_bytes(fig_tree)
        if tree_png:
            export_assets["editado/03_treemap.png"] = tree_png

    if "Nube de palabras" in viz_types and not df_plot.empty:
        with st.expander("Editar: Nube de palabras", expanded=False):
            t1, t2, t3, t4 = st.columns(4)
            with t1:
                topic_options = df_plot["Item"].astype(str).tolist()
                selected_topic = st.selectbox(f"{item_label} para nube", options=topic_options, index=0, key=f"{key_prefix}_wc_topic")
            with t2:
                max_words = st.slider("Max palabras", min_value=20, max_value=300, value=120, step=10, key=f"{key_prefix}_wc_words")
            with t3:
                bg = st.selectbox("Fondo", options=["white", "black"], index=0, key=f"{key_prefix}_wc_bg")
            with t4:
                wc_height = st.slider("Alto nube (px)", min_value=250, max_value=900, value=420, step=10, key=f"{key_prefix}_wc_height")

        text_col = "Texto.Mejorado" if "Texto.Mejorado" in df_corr.columns else "Texto.Original"
        topic_cols = [c for c in df_corr.columns if c.startswith("Topico_")]
        topic_cols = sorted(
            topic_cols,
            key=lambda x: int(x.split("_")[-1]) if x.split("_")[-1].isdigit() else x,
        )
        if not topic_cols:
            st.warning("No se encontraron columnas Topico_* para construir nube de palabras.")
        else:
            mask = False
            for col in topic_cols:
                mask = mask | (df_corr[col].apply(clean_text_value) == selected_topic)
            textos = df_corr.loc[mask, text_col].dropna().astype(str).tolist()

            if not textos:
                st.info(f"No hay textos para ese {item_label.lower()} con el filtro actual.")
            else:
                stop = set(STOPWORDS)
                stop.update(["la", "el", "de", "que", "y", "en", "por", "con", "los", "las", "un", "una"])
                wc = WordCloud(
                    width=1200,
                    height=wc_height,
                    background_color=bg,
                    max_words=max_words,
                    stopwords=stop,
                ).generate(" ".join(textos))
                wc_array = wc.to_array()
                st.image(wc_array, caption=f"Nube de palabras - {selected_topic}", use_column_width=True)
                img_buf = BytesIO()
                Image.fromarray(wc_array).save(img_buf, format="PNG")
                export_assets["editado/04_nube_palabras.png"] = img_buf.getvalue()

    if "Heatmap" in viz_types:
        with st.expander("Editar: Heatmap", expanded=False):
            h1, h2, h3 = st.columns(3)
            with h1:
                hm_mode = st.selectbox(
                    "Modo",
                    options=["Contexto x Posicion", "Coocurrencia Contexto x Contexto"],
                    index=0,
                    key=f"{key_prefix}_hm_mode",
                )
            with h2:
                hm_height = st.slider("Alto heatmap (px)", min_value=320, max_value=1000, value=560, step=10, key=f"{key_prefix}_hm_height")
            with h3:
                hm_color = st.selectbox(
                    "Escala color",
                    options=["Blues", "Viridis", "Cividis", "Plasma", "Magma", "Turbo"],
                    index=0,
                    key=f"{key_prefix}_hm_color",
                )

        topic_cols = [c for c in df_corr.columns if c.startswith("Topico_")]
        if not topic_cols:
            st.warning("No se encontraron columnas Topico_* para construir heatmap.")
        else:
            if hm_mode == "Contexto x Posicion":
                hm_df = df_corr[topic_cols].copy()
                melted = hm_df.melt(var_name="Posicion", value_name="Contexto")
                melted["Contexto"] = melted["Contexto"].apply(clean_text_value)
                melted = melted[melted["Contexto"] != ""]
                mat = pd.crosstab(melted["Contexto"], melted["Posicion"])
                mat = mat.reindex(columns=topic_cols, fill_value=0)
                mat = mat.reindex(mat.sum(axis=1).sort_values(ascending=False).index).head(top_n)

                if mat.empty:
                    st.info("No hay datos suficientes para el heatmap de posicion.")
                else:
                    fig_hm = px.imshow(
                        mat,
                        labels=dict(x="Posicion", y="Contexto", color="Frecuencia"),
                        color_continuous_scale=hm_color,
                        title="Heatmap: Contexto x Posicion",
                        aspect="auto",
                    )
                    fig_hm.update_layout(height=hm_height)
                    st.plotly_chart(fig_hm, use_container_width=True)
                    hm_png = fig_to_png_bytes(fig_hm)
                    if hm_png:
                        export_assets["editado/05_heatmap_posicion.png"] = hm_png
            else:
                pairs = {}
                for _, row in df_corr.iterrows():
                    vals = []
                    for c in topic_cols:
                        v = clean_text_value(row.get(c, ""))
                        if v:
                            vals.append(v)
                    vals = list(dict.fromkeys(vals))
                    for i in range(len(vals)):
                        for j in range(i + 1, len(vals)):
                            a, b = sorted([vals[i], vals[j]])
                            pairs[(a, b)] = pairs.get((a, b), 0) + 1

                if not pairs:
                    st.info("No hay coocurrencias para construir el heatmap.")
                else:
                    top_topics = df_plot["Item"].astype(str).tolist()
                    mat = pd.DataFrame(0, index=top_topics, columns=top_topics)
                    for (a, b), w in pairs.items():
                        if a in mat.index and b in mat.columns:
                            mat.loc[a, b] += w
                            mat.loc[b, a] += w

                    fig_hm = px.imshow(
                        mat,
                        labels=dict(x="Contexto", y="Contexto", color="Coocurrencia"),
                        color_continuous_scale=hm_color,
                        title="Heatmap: Coocurrencia Contexto x Contexto",
                        aspect="auto",
                    )
                    fig_hm.update_layout(height=hm_height)
                    st.plotly_chart(fig_hm, use_container_width=True)
                    hm_png = fig_to_png_bytes(fig_hm)
                    if hm_png:
                        export_assets["editado/05_heatmap_coocurrencia.png"] = hm_png

    if "Sankey" in viz_types:
        with st.expander("Editar: Sankey", expanded=False):
            s1, s2, s3, s4 = st.columns(4)
            with s1:
                sankey_mode = st.selectbox(
                    "Flujo",
                    options=["Contexto_1 -> Contexto_2", "Cadena adyacente completa"],
                    index=1,
                    key=f"{key_prefix}_sankey_mode",
                )
            with s2:
                sankey_min = st.slider("Min enlaces", min_value=1, max_value=20, value=2, step=1, key=f"{key_prefix}_sankey_min")
            with s3:
                sankey_pad = st.slider("Espaciado nodos", min_value=5, max_value=40, value=14, step=1, key=f"{key_prefix}_sankey_pad")
            with s4:
                sankey_height = st.slider("Alto sankey (px)", min_value=360, max_value=1100, value=620, step=10, key=f"{key_prefix}_sankey_height")
            s5, s6, s7, s8 = st.columns(4)
            with s5:
                sankey_max_links = st.slider("Max enlaces", min_value=10, max_value=300, value=80, step=10, key=f"{key_prefix}_sankey_max_links")
            with s6:
                sankey_label_len = st.slider("Largo etiqueta", min_value=10, max_value=60, value=28, step=1, key=f"{key_prefix}_sankey_label_len")
            with s7:
                sankey_thickness = st.slider("Grosor nodos", min_value=8, max_value=40, value=16, step=1, key=f"{key_prefix}_sankey_thickness")
            with s8:
                sankey_link_alpha = st.slider("Opacidad enlaces", min_value=0.1, max_value=1.0, value=0.35, step=0.05, key=f"{key_prefix}_sankey_alpha")
            s9, s10, s11, s12 = st.columns(4)
            with s9:
                sankey_font_size = st.slider("Tamano letra", min_value=10, max_value=26, value=15, step=1, key=f"{key_prefix}_sankey_font_size")
            with s10:
                sankey_text_color = st.selectbox(
                    "Color texto",
                    options=["Negro", "Azul oscuro", "Gris oscuro"],
                    index=0,
                    key=f"{key_prefix}_sankey_text_color",
                )
            with s11:
                sankey_font_family = st.selectbox(
                    "Tipografia",
                    options=["Arial", "Verdana", "Tahoma", "Trebuchet MS"],
                    index=1,
                    key=f"{key_prefix}_sankey_font_family",
                )
            with s12:
                sankey_compact_labels = st.checkbox("Etiquetas compactas", value=True, key=f"{key_prefix}_sankey_compact_labels")
            sankey_hide_self = st.checkbox("Ocultar auto-enlaces", value=True, key=f"{key_prefix}_sankey_hide_self")
            s13, s14, s15, s16 = st.columns(4)
            with s13:
                show_only_top_flows = st.checkbox("Mostrar solo top flujos", value=True, key=f"{key_prefix}_sankey_top_only")
            with s14:
                top_n_flows = st.slider("Top N flujos", min_value=10, max_value=500, value=120, step=10, key=f"{key_prefix}_sankey_top_n")
            with s15:
                min_link_pct = st.slider("Min % relativo enlace", min_value=0.0, max_value=10.0, value=0.8, step=0.1, key=f"{key_prefix}_sankey_min_pct")
            with s16:
                group_similar_labels = st.checkbox("Agrupar etiquetas similares", value=True, key=f"{key_prefix}_sankey_group_sim")
            s17, s18 = st.columns(2)
            with s17:
                similarity_threshold = st.slider("Umbral similitud etiquetas", min_value=0.70, max_value=0.99, value=0.86, step=0.01, key=f"{key_prefix}_sankey_sim_thr")
            with s18:
                sankey_arrangement = st.selectbox(
                    "Distribucion",
                    options=["snap", "perpendicular", "freeform", "fixed"],
                    index=0,
                    key=f"{key_prefix}_sankey_arrangement",
                )

        topic_cols = [c for c in df_corr.columns if c.startswith("Topico_")]
        if len(topic_cols) < 2:
            st.warning("Se requieren al menos dos columnas Topico_* para Sankey.")
        else:
            topic_cols = sorted(topic_cols, key=lambda x: int(x.split("_")[-1]) if x.split("_")[-1].isdigit() else x)
            all_labels = []
            for _, row in df_corr.iterrows():
                for c in topic_cols:
                    v = clean_text_value(row.get(c, ""))
                    if v:
                        all_labels.append(v)

            sim_map = {}
            if group_similar_labels:
                sim_map = build_similar_label_map(all_labels, similarity_threshold)

            edge_stats: dict[tuple[int, str, int, str], dict[str, object]] = {}
            for _, row in df_corr.iterrows():
                seq: list[tuple[int, str]] = []
                for idx_col, c in enumerate(topic_cols):
                    v = normalize_label_text(row.get(c, ""))
                    if not v:
                        continue
                    if group_similar_labels and sim_map:
                        v = sim_map.get(v, v)
                    seq.append((idx_col, v))

                if len(seq) < 2:
                    continue

                root_label = seq[0][1]
                if sankey_mode == "Contexto_1 -> Contexto_2":
                    pairs = [(seq[0], seq[1])]
                else:
                    pairs = [(seq[i], seq[i + 1]) for i in range(len(seq) - 1)]

                for (src_col, a), (tgt_col, b) in pairs:
                    if sankey_hide_self and a == b:
                        continue
                    k = (src_col, a, tgt_col, b)
                    if k not in edge_stats:
                        edge_stats[k] = {"value": 0, "roots": Counter()}
                    edge_stats[k]["value"] = int(edge_stats[k]["value"]) + 1
                    edge_stats[k]["roots"][root_label] += 1

            rows_links = []
            for (src_col, a, tgt_col, b), data in edge_stats.items():
                rows_links.append(
                    {
                        "src_col": src_col,
                        "src": a,
                        "tgt_col": tgt_col,
                        "tgt": b,
                        "value": int(data["value"]),
                        "root": data["roots"].most_common(1)[0][0] if data["roots"] else a,
                    }
                )

            # Filtro de ruido por conteo y porcentaje relativo (visual).
            rows_links = [r for r in rows_links if r["value"] >= sankey_min]
            total_links_weight = sum(r["value"] for r in rows_links)
            if min_link_pct > 0 and total_links_weight > 0:
                rows_links = [r for r in rows_links if (r["value"] / total_links_weight * 100.0) >= min_link_pct]

            rows_links = sorted(rows_links, key=lambda r: r["value"], reverse=True)
            if show_only_top_flows:
                rows_links = rows_links[:top_n_flows]

            if not rows_links:
                st.info("No hay enlaces Sankey con el umbral configurado.")
            else:
                rows_links = rows_links[:sankey_max_links]
                total_links_weight = sum(r["value"] for r in rows_links)

                node_keys = set()
                for r in rows_links:
                    node_keys.add((r["src_col"], r["src"]))
                    node_keys.add((r["tgt_col"], r["tgt"]))

                node_strength = defaultdict(int)
                node_group_votes = defaultdict(Counter)
                group_weights = Counter()
                for r in rows_links:
                    group = r["root"]
                    group_weights[group] += r["value"]
                    node_strength[(r["src_col"], r["src"])] += r["value"]
                    node_strength[(r["tgt_col"], r["tgt"])] += r["value"]
                    node_group_votes[(r["src_col"], r["src"])][group] += r["value"]
                    node_group_votes[(r["tgt_col"], r["tgt"])][group] += r["value"]

                cols_order = sorted({c for c, _ in node_keys})
                n_cols = max(1, len(cols_order))
                x_pos_map = {c: (0.0 if n_cols == 1 else i / (n_cols - 1)) for i, c in enumerate(cols_order)}

                nodes_ordered: list[tuple[int, str]] = []
                y_map: dict[tuple[int, str], float] = {}
                for c in cols_order:
                    in_col = [k for k in node_keys if k[0] == c]
                    in_col = sorted(in_col, key=lambda k: (-node_strength[k], k[1]))
                    for i_node, nk in enumerate(in_col):
                        y_map[nk] = (i_node + 1) / (len(in_col) + 1)
                        nodes_ordered.append(nk)

                idx = {nk: i for i, nk in enumerate(nodes_ordered)}
                labels_full = [nk[1] for nk in nodes_ordered]
                labels_short = [smart_truncate_label(l, sankey_label_len) for l in labels_full] if sankey_compact_labels else labels_full
                node_x = [x_pos_map[nk[0]] for nk in nodes_ordered]
                node_y = [y_map[nk] for nk in nodes_ordered]

                palette = get_shared_report_theme()["group_palette"]
                group_sorted = [g for g, _ in group_weights.most_common()]
                group_color_map = {g: palette[i % len(palette)] for i, g in enumerate(group_sorted)}

                node_colors = []
                for nk in nodes_ordered:
                    g = node_group_votes[nk].most_common(1)[0][0] if node_group_votes[nk] else nk[1]
                    node_colors.append(hex_to_rgba(group_color_map.get(g, "#5A6B7D"), 0.92))

                values = [r["value"] for r in rows_links]
                v_min = min(values) if values else 0
                v_max = max(values) if values else 1

                source = [idx[(r["src_col"], r["src"])] for r in rows_links]
                target = [idx[(r["tgt_col"], r["tgt"])] for r in rows_links]
                value = [r["value"] for r in rows_links]
                link_custom = []
                link_colors = []
                for r in rows_links:
                    pct = (r["value"] / total_links_weight * 100.0) if total_links_weight else 0.0
                    link_custom.append(f"{r['src']} -> {r['tgt']}<br>Frecuencia: {r['value']}<br>% del flujo mostrado: {pct:.1f}%")
                    # Intensidad visual por peso: enlaces fuertes mas notorios, debiles mas tenues.
                    intensity = 1.0 if v_max <= v_min else (r["value"] - v_min) / (v_max - v_min)
                    alpha = max(0.08, min(0.95, sankey_link_alpha * (0.40 + 0.95 * intensity)))
                    base_hex = group_color_map.get(r["root"], "#6C7A89")
                    link_colors.append(hex_to_rgba(base_hex, alpha))

                node_custom = []
                for nk in nodes_ordered:
                    wt = node_strength[nk]
                    pct = (wt / total_links_weight * 100.0) if total_links_weight else 0.0
                    node_custom.append(f"{nk[1]}<br>Nivel: {nk[0] + 1}<br>Flujo asociado: {wt}<br>% del flujo mostrado: {pct:.1f}%")

                text_color_map = {
                    "Negro": "#111111",
                    "Azul oscuro": "#0b2c4d",
                    "Gris oscuro": "#2f2f2f",
                }

                fig_sankey = go.Figure(
                    data=[
                        go.Sankey(
                            node=dict(
                                pad=sankey_pad,
                                thickness=sankey_thickness,
                                line=dict(color="rgba(80,80,80,0.25)", width=0.6),
                                label=labels_short,
                                customdata=node_custom,
                                color=node_colors,
                                x=node_x,
                                y=node_y,
                                hovertemplate="%{customdata}<extra></extra>",
                            ),
                            link=dict(
                                source=source,
                                target=target,
                                value=value,
                                color=link_colors,
                                customdata=link_custom,
                                hovertemplate="%{customdata}<extra></extra>",
                            ),
                            arrangement=sankey_arrangement,
                        )
                    ]
                )
                fig_sankey.update_layout(
                    title_text="Sankey de contextos",
                    font=dict(
                        size=sankey_font_size,
                        color=text_color_map[sankey_text_color],
                        family=sankey_font_family,
                    ),
                    height=sankey_height,
                    margin=dict(l=8, r=8, t=48, b=8),
                )

                # Resumen ejecutivo para lectura rapida de insights.
                strongest = max(rows_links, key=lambda r: r["value"])
                strongest_txt = f"{smart_truncate_label(strongest['src'], 38)} -> {smart_truncate_label(strongest['tgt'], 38)}"
                m1, m2, m3 = st.columns(3)
                with m1:
                    st.metric("Flujos mostrados", f"{len(rows_links)}")
                with m2:
                    st.metric("Nodos mostrados", f"{len(nodes_ordered)}")
                with m3:
                    st.metric("Flujo mas fuerte", f"n={strongest['value']}")
                st.caption(f"Flujo principal: {strongest_txt} (n={strongest['value']})")

                st.plotly_chart(fig_sankey, use_container_width=True)
                sankey_png = fig_to_png_bytes(fig_sankey)
                if sankey_png:
                    export_assets["editado/06_sankey.png"] = sankey_png

    html_red = OUTPUT_DIR / "graficos" / "red_interactiva.html"
    if "Red interactiva" in viz_types and html_red.exists():
        with st.expander("Editar: Red interactiva", expanded=False):
            net_height = st.slider("Alto red (px)", min_value=500, max_value=1400, value=900, step=20, key=f"{key_prefix}_net_height")
        with st.expander("Ver red interactiva generada (Paso 5)", expanded=True):
            html_content = html_red.read_text(encoding="utf-8", errors="replace")
            components.html(html_content, height=net_height, scrolling=True)
            export_assets["editado/07_red_interactiva.html"] = html_content.encode("utf-8")

    # Incluye siempre los graficos originales del pipeline (si existen).
    original_dir = OUTPUT_DIR / "graficos"
    if original_dir.exists():
        for p in original_dir.glob("*"):
            if p.is_file() and p.suffix.lower() in {".png", ".html"}:
                export_assets[f"original/{p.name}"] = p.read_bytes()

    if export_assets:
        st.markdown("**Exportacion**")
        zip_bytes = build_zip_bytes(export_assets)
        st.download_button(
            "Exportar todos (original + editado) (.zip)",
            data=zip_bytes,
            file_name="textana_graficos_original_y_editado.zip",
            mime="application/zip",
        )

    if not show_cross_section:
        return

    # ---------------- Cruce por variable ----------------
    st.divider()
    st.subheader("Cruce de resultados por variable")

    reserved_prefixes = ("Topico_", "COD_Topico_", "Afinidad_", "Razon_", "Validacion_", "Ind.")
    reserved_names = {
        "IDGD",
        "Texto.Original",
        "Texto.Mejorado",
        "Resumen",
        "Frecuencia",
        "Porcentaje",
        "Topico",
        "COD_Topico",
        "Variable",
    }
    candidate_vars = []
    for col in df_corr.columns:
        if col in reserved_names:
            continue
        if any(col.startswith(pref) for pref in reserved_prefixes):
            continue
        non_null = df_corr[col].dropna()
        if non_null.empty:
            continue
        uniq = non_null.astype(str).nunique()
        # Permite al usuario decidir cruces; solo exige variabilidad minima.
        if uniq >= 2:
            candidate_vars.append(col)

    if not candidate_vars:
        st.info("No se encontraron variables de segmentacion disponibles. Asegura que viajen en el pipeline desde el Excel base.")
        return

    cv1, cv2, cv3, cv4 = st.columns(4)
    selected_cross_vars = []
    available_sorted = sorted(candidate_vars)
    if selected_cross_vars_param:
        selected_cross_vars = [v for v in selected_cross_vars_param if v in available_sorted]
    with cv1:
        st.text_input(
            "Variables de cruce (desde parametros)",
            value=", ".join(selected_cross_vars) if selected_cross_vars else "(ninguna seleccionada)",
            disabled=True,
            key=f"{key_prefix}_cross_vars_readonly",
        )
    with cv2:
        cross_top_n = st.slider(
            "Top N topicos (cruce)",
            min_value=3,
            max_value=min(20, max(3, len(df_plot))),
            value=min(8, max(3, len(df_plot))),
            key=f"{key_prefix}_cross_topn",
        )
    with cv3:
        cross_mode = st.selectbox("Modo", options=["Frecuencia", "Porcentaje fila"], index=1, key=f"{key_prefix}_cross_mode")
    with cv4:
        cross_hm_color = st.selectbox(
            "Color heatmap cruces",
            options=["Viridis", "Blues", "Cividis", "Plasma", "Magma", "Turbo"],
            index=0,
            key=f"{key_prefix}_cross_hm_color",
        )

    cross_graphs = st.multiselect(
        "Graficos de cruce",
        options=["Barras apiladas", "Heatmap", "Sankey"],
        default=["Barras apiladas", "Heatmap", "Sankey"],
        key=f"{key_prefix}_cross_graphs",
    )
    if "Sankey" in cross_graphs:
        with st.expander("Editar Sankey cruces", expanded=False):
            sk1, sk2, sk3, sk4 = st.columns(4)
            with sk1:
                cross_sk_min_flow = st.slider("Min flujo", min_value=1, max_value=50, value=2, step=1, key=f"{key_prefix}_cross_sk_min")
            with sk2:
                cross_sk_max_links = st.slider("Max enlaces", min_value=10, max_value=400, value=120, step=10, key=f"{key_prefix}_cross_sk_max")
            with sk3:
                cross_sk_height = st.slider("Alto Sankey (px)", min_value=360, max_value=1200, value=620, step=10, key=f"{key_prefix}_cross_sk_height")
            with sk4:
                cross_sk_alpha = st.slider("Opacidad enlaces", min_value=0.10, max_value=1.0, value=0.35, step=0.05, key=f"{key_prefix}_cross_sk_alpha")
            sk5, sk6, sk7, sk8 = st.columns(4)
            with sk5:
                cross_sk_label_len = st.slider("Largo etiqueta", min_value=8, max_value=60, value=24, step=1, key=f"{key_prefix}_cross_sk_lbl_len")
            with sk6:
                cross_sk_node_pad = st.slider("Espaciado nodos", min_value=5, max_value=40, value=14, step=1, key=f"{key_prefix}_cross_sk_pad")
            with sk7:
                cross_sk_node_thickness = st.slider("Grosor nodos", min_value=8, max_value=40, value=16, step=1, key=f"{key_prefix}_cross_sk_thick")
            with sk8:
                cross_sk_font_size = st.slider("Tamano letra", min_value=9, max_value=24, value=12, step=1, key=f"{key_prefix}_cross_sk_font")
            sk9, sk10, sk11 = st.columns(3)
            with sk9:
                cross_sk_top_segments = st.slider("Top segmentos", min_value=2, max_value=30, value=10, step=1, key=f"{key_prefix}_cross_sk_top_seg")
            with sk10:
                cross_sk_top_topics = st.slider("Top topicos", min_value=3, max_value=30, value=12, step=1, key=f"{key_prefix}_cross_sk_top_top")
            with sk11:
                cross_sk_arrangement = st.selectbox(
                    "Arreglo",
                    options=["snap", "perpendicular", "freeform", "fixed"],
                    index=0,
                    key=f"{key_prefix}_cross_sk_arrange",
                )

    topic_cols = [c for c in df_corr.columns if c.startswith("Topico_")]
    if not topic_cols:
        st.info("No hay Topico_* para construir cruces.")
        return

    if not selected_cross_vars:
        st.info("Selecciona una o mas variables de cruce en la seccion de parametros (parte superior).")
        return

    for cross_var in selected_cross_vars:
        st.markdown(f"#### Cruce por: `{cross_var}`")
        rows = []
        for _, r in df_corr.iterrows():
            seg = clean_text_value(r.get(cross_var, ""))
            if not seg:
                continue
            seen = set()
            for c in topic_cols:
                t = clean_text_value(r.get(c, ""))
                if not t or t in seen:
                    continue
                seen.add(t)
                rows.append({"Segmento": seg, "Topico": t})

        if not rows:
            st.info(f"No hay datos para cruce con `{cross_var}`.")
            continue

        cross_df = pd.DataFrame(rows)
        top_topics_cross = cross_df["Topico"].value_counts().head(cross_top_n).index.tolist()
        cross_df = cross_df[cross_df["Topico"].isin(top_topics_cross)]

        mat = pd.crosstab(cross_df["Segmento"], cross_df["Topico"])
        mat = mat.reindex(sorted(mat.index), axis=0)
        if cross_mode == "Porcentaje fila":
            mat = mat.div(mat.sum(axis=1).replace(0, 1), axis=0) * 100
            value_label = "% dentro de segmento"
        else:
            value_label = "Frecuencia"

        fig_cross_bar = None
        fig_cross_hm = None
        fig_cross_sankey = None

        if "Barras apiladas" in cross_graphs:
            cross_long = mat.reset_index().melt(id_vars="Segmento", var_name="Topico", value_name="Valor")
            fig_cross_bar = px.bar(
                cross_long,
                x="Segmento",
                y="Valor",
                color="Topico",
                barmode="stack",
                title=f"Topicos por {cross_var}",
                labels={"Valor": value_label},
            )
            fig_cross_bar.update_layout(height=520)
            if cross_mode == "Porcentaje fila":
                fig_cross_bar.update_traces(
                    texttemplate="%{y:.0f}%",
                    textposition="inside",
                    hovertemplate="%{x}<br>%{fullData.name}: %{y:.0f}%<extra></extra>",
                )
            else:
                fig_cross_bar.update_traces(
                    texttemplate="%{y}",
                    textposition="inside",
                    hovertemplate="%{x}<br>%{fullData.name}: %{y}<extra></extra>",
                )
            st.plotly_chart(fig_cross_bar, use_container_width=True)

        if "Heatmap" in cross_graphs:
            fig_cross_hm = px.imshow(
                mat,
                labels=dict(x="Topico", y=cross_var, color=value_label),
                color_continuous_scale=cross_hm_color,
                title=f"Heatmap de cruce: {cross_var} x Topico",
                aspect="auto",
            )
            fig_cross_hm.update_layout(height=520)
            st.plotly_chart(fig_cross_hm, use_container_width=True)

        if "Sankey" in cross_graphs:
            # Flujo variable de cruce -> topico
            sankey_links = cross_df.groupby(["Segmento", "Topico"]).size().reset_index(name="Valor")
            if cross_mode == "Porcentaje fila":
                seg_tot = sankey_links.groupby("Segmento")["Valor"].transform("sum").replace(0, 1)
                sankey_links["Valor"] = sankey_links["Valor"] / seg_tot * 100

            # Limpieza visual por relevancia de nodos
            top_segments = (
                sankey_links.groupby("Segmento")["Valor"]
                .sum()
                .sort_values(ascending=False)
                .head(cross_sk_top_segments)
                .index
                .tolist()
            )
            top_topics = (
                sankey_links.groupby("Topico")["Valor"]
                .sum()
                .sort_values(ascending=False)
                .head(cross_sk_top_topics)
                .index
                .tolist()
            )
            sankey_links = sankey_links[
                sankey_links["Segmento"].isin(top_segments) & sankey_links["Topico"].isin(top_topics)
            ]
            sankey_links = sankey_links[sankey_links["Valor"] >= cross_sk_min_flow]
            sankey_links = sankey_links.sort_values("Valor", ascending=False).head(cross_sk_max_links)
            if sankey_links.empty:
                st.info(f"Sankey de `{cross_var}` sin enlaces con los parametros actuales.")
                fig_cross_sankey = None
            else:
                labels_left = sorted(sankey_links["Segmento"].unique().tolist())
                labels_right = sorted(sankey_links["Topico"].unique().tolist())
                labels = labels_left + labels_right
                idx = {lab: i for i, lab in enumerate(labels)}
                labels_short = [shorten_label(l, cross_sk_label_len) for l in labels]

                fig_cross_sankey = go.Figure(
                    data=[
                        go.Sankey(
                            node=dict(
                                pad=cross_sk_node_pad,
                                thickness=cross_sk_node_thickness,
                                label=labels_short,
                                customdata=labels,
                                color=["rgba(55,126,184,0.88)"] * len(labels_left) + ["rgba(77,175,74,0.88)"] * len(labels_right),
                                hovertemplate="%{customdata}<extra></extra>",
                            ),
                            link=dict(
                                source=[idx[s] for s in sankey_links["Segmento"]],
                                target=[idx[t] for t in sankey_links["Topico"]],
                                value=sankey_links["Valor"].tolist(),
                                color=f"rgba(130,130,130,{cross_sk_alpha})",
                            ),
                            arrangement=cross_sk_arrangement,
                        )
                    ]
                )
                sankey_title_suffix = "(% dentro de segmento)" if cross_mode == "Porcentaje fila" else "(frecuencia)"
                fig_cross_sankey.update_layout(
                    title_text=f"Sankey de cruce: {cross_var} -> Topico {sankey_title_suffix}",
                    height=cross_sk_height,
                    font_size=cross_sk_font_size,
                )
                st.plotly_chart(fig_cross_sankey, use_container_width=True)

        # Agrega export por variable de cruce.
        var_slug = "".join(ch if ch.isalnum() else "_" for ch in str(cross_var))[:40] or "variable"
        if fig_cross_bar is not None:
            cross_bar_png = fig_to_png_bytes(fig_cross_bar)
            if cross_bar_png:
                export_assets[f"editado/08_cruce_barras_{var_slug}.png"] = cross_bar_png
        if fig_cross_hm is not None:
            cross_hm_png = fig_to_png_bytes(fig_cross_hm)
            if cross_hm_png:
                export_assets[f"editado/09_cruce_heatmap_{var_slug}.png"] = cross_hm_png
        if fig_cross_sankey is not None:
            cross_sk_png = fig_to_png_bytes(fig_cross_sankey)
            if cross_sk_png:
                export_assets[f"editado/10_cruce_sankey_{var_slug}.png"] = cross_sk_png

    st.markdown("**Exportacion con cruces**")
    zip_bytes_cross = build_zip_bytes(export_assets)
    st.download_button(
        "Exportar todos + cruces (original + editado) (.zip)",
        data=zip_bytes_cross,
        file_name="textana_graficos_y_cruces_original_y_editado.zip",
        mime="application/zip",
    )


def run_pipeline(
    input_file: Path,
    sheet: str,
    cols: str,
    token_file: str,
    max_words: int,
    threads_mejora: int,
    threads_validacion: int,
    max_topicos: int,
    max_contextos: int,
    export_spss: bool,
    progress_bar,
    status_box,
    eta_box,
    report_box,
    step_progress_box,
) -> tuple[int, str, pd.DataFrame]:
    cmd = [
        sys.executable,
        "-u",
        str(ROOT / "src" / "main.py"),
        "--input-file",
        str(input_file),
        "--sheet",
        sheet,
        "--cols",
        cols,
        "--token-file",
        token_file,
        "--max-words",
        str(max_words),
        "--threads-mejora",
        str(threads_mejora),
        "--threads-validacion",
        str(threads_validacion),
        "--max-topicos",
        str(max_topicos),
        "--max-contextos",
        str(max_contextos),
        "--output-dir",
        str(OUTPUT_DIR),
    ]

    if not export_spss:
        cmd.append("--skip-spss")

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(ROOT),
        env=env,
    )

    planned_steps, report = init_step_report(export_spss)
    step_alias_map = {
        "paso1": "paso1_lectura.py",
        "paso2": "paso2_mejora.py",
        "paso3": "paso3_clasificacion.py",
        "paso4": "paso4_export_spss.py",
        "paso5": "paso5_graficos.py",
    }
    step_progress: dict[str, dict[str, int]] = {
        step: {"done": 0, "total": 0, "remaining": 0} for step in planned_steps
    }

    def render_step_progress() -> None:
        with step_progress_box.container():
            st.subheader("Avance por paso")
            for step in planned_steps:
                label = STEP_LABELS.get(step, step)
                p = step_progress[step]
                total = p["total"]
                done = p["done"]
                remaining = p["remaining"]
                if total > 0:
                    frac = min(1.0, max(0.0, done / total))
                    txt = f"{label}: {done}/{total} - faltan {remaining} registros"
                else:
                    status = report[step]["status"]
                    frac = 1.0 if status in {"completado", "omitido"} else 0.0
                    txt = f"{label}: {status}"
                st.progress(frac, text=txt)

    render_step_progress()
    current_step: str | None = None
    logs: list[str] = []

    while True:
        line = proc.stdout.readline() if proc.stdout else ""
        if line:
            line = line.rstrip("\n")
            logs.append(line)
            clean = line.strip()

            if clean.startswith("[RUN] "):
                script = clean.replace("[RUN] ", "", 1).strip()
                if script in report:
                    current_step = script
                    report[script]["status"] = "ejecutando"
                    report[script]["start"] = time.time()
                    status_box.info(f"Ejecutando: {report[script]['step']}")

            if clean.startswith("[OK] "):
                script = clean.replace("[OK] ", "", 1).strip()
                if script in report:
                    report[script]["status"] = "completado"
                    report[script]["end"] = time.time()
                    if step_progress[script]["total"] == 0:
                        step_progress[script] = {"done": 1, "total": 1, "remaining": 0}
                    if report[script]["start"] is not None:
                        report[script]["duration_s"] = report[script]["end"] - report[script]["start"]
                    if current_step == script:
                        current_step = None

            if "Paso 4 (SPSS) omitido" in clean and "paso4_export_spss.py" in report:
                report["paso4_export_spss.py"]["status"] = "omitido"
                report["paso4_export_spss.py"]["duration_s"] = 0.0
                step_progress["paso4_export_spss.py"] = {"done": 1, "total": 1, "remaining": 0}

            p_evt = parse_progress_line(clean)
            if p_evt:
                alias = str(p_evt.get("step", ""))
                script = step_alias_map.get(alias)
                if script in step_progress:
                    done_p = int(p_evt.get("done", 0))
                    total_p = int(p_evt.get("total", 0))
                    rem_p = int(p_evt.get("remaining", max(0, total_p - done_p)))
                    step_progress[script] = {"done": done_p, "total": total_p, "remaining": rem_p}

            done = sum(1 for step in planned_steps if report[step]["status"] in {"completado", "omitido"})
            progress = done / max(1, len(planned_steps))
            progress_bar.progress(progress)
            eta_box.caption(calculate_eta(report, current_step))
            report_box.dataframe(report_to_dataframe(report), use_container_width=True, hide_index=True)
            render_step_progress()

        if proc.poll() is not None:
            break

    if proc.stdout:
        rem = proc.stdout.read()
        if rem:
            logs.extend(rem.splitlines())

    code = proc.returncode or 0

    if code != 0 and current_step and current_step in report:
        report[current_step]["status"] = "fallo"
        report[current_step]["end"] = time.time()
        if report[current_step]["start"] is not None:
            report[current_step]["duration_s"] = report[current_step]["end"] - report[current_step]["start"]

    final_progress = 1.0 if code == 0 else sum(
        1 for step in planned_steps if report[step]["status"] in {"completado", "omitido"}
    ) / max(1, len(planned_steps))
    progress_bar.progress(final_progress)
    render_step_progress()
    return code, "\n".join(logs), report_to_dataframe(report)


def main() -> None:
    st.set_page_config(page_title="Textana V5", layout="wide")
    apply_layout_tweaks()
    st.title("Textana V5 - Entorno visual")
    st.write("Carga un Excel, selecciona hoja/columnas y ejecuta el pipeline con reporte por pasos.")

    if "result_excel_path" not in st.session_state:
        st.session_state["result_excel_path"] = ""
    if "allow_graphs_from_run" not in st.session_state:
        st.session_state["allow_graphs_from_run"] = False
    if "allow_graphs_from_upload" not in st.session_state:
        st.session_state["allow_graphs_from_upload"] = False

    uploaded = st.file_uploader("Archivo Excel de entrada", type=["xlsx", "xls"])

    excel_path = None
    sheets: list[str] = []
    columns_map: dict[str, list[str]] = {}

    if uploaded:
        try:
            excel_path = save_upload(uploaded)
            sheets, columns_map = get_excel_metadata(excel_path)
        except Exception as exc:
            st.error(f"No se pudo leer el Excel: {exc}")
            return

    c1, c2, c3, c4 = st.columns([1.2, 1.0, 1.0, 0.8])
    with c1:
        if sheets:
            sheet = st.selectbox("Hoja", options=sheets, index=0)
            columnas_disponibles = columns_map.get(sheet, [])
            cols_sel = st.multiselect(
                "Columna(s) de texto",
                options=columnas_disponibles,
                default=columnas_disponibles[:1] if columnas_disponibles else [],
            )
        else:
            sheet = st.text_input("Hoja", value="Hoja1")
            cols_sel = []
    with c2:
        max_words = st.number_input("Max words", min_value=5, max_value=60, value=20)
        threads_mejora = st.number_input("Hilos Paso 2", min_value=1, max_value=32, value=5)
        threads_validacion = st.number_input("Hilos Paso 3", min_value=1, max_value=64, value=8)
    with c3:
        max_topicos = st.number_input("Max topicos", min_value=1, max_value=20, value=5)
        max_contextos = st.number_input("Max contextos", min_value=1, max_value=20, value=3)
        token_file = st.text_input("Archivo token", value="tokenkey.txt")
    with c4:
        log_height = st.slider("Alto log (px)", min_value=180, max_value=700, value=320, step=20)

    cross_vars_param: list[str] = []
    if sheets:
        st.markdown("#### Parametros de cruce (solo graficos y analisis)")
        selectable_cross_cols = [c for c in columnas_disponibles if c not in cols_sel and c not in {"IDGD"}]
        cross_vars_param = st.multiselect(
            "Variables para cruces",
            options=selectable_cross_cols,
            default=[],
            help="Estas variables solo afectan los cruces analiticos en graficos. No cambian la codificacion Textana.",
            key="cross_vars_param",
        )

    py313_or_newer = sys.version_info >= (3, 13)
    export_spss = st.checkbox("Exportar SPSS (Paso 4)", value=not py313_or_newer)
    if py313_or_newer and export_spss:
        st.warning("En Python 3.13 pyreadstat puede fallar. Si ocurre, desactiva SPSS.")

    run = st.button("Ejecutar pipeline", type="primary", use_container_width=True)

    if run:
        if not uploaded or excel_path is None:
            st.error("Sube un archivo Excel antes de ejecutar.")
            return

        if not cols_sel:
            st.error("Selecciona al menos una columna de texto.")
            return

        cols = ",".join(cols_sel)

        st.subheader("Progreso")
        progress_bar = st.progress(0.0)
        status_box = st.empty()
        eta_box = st.empty()

        st.subheader("Reporte por pasos")
        report_box = st.empty()
        step_progress_box = st.empty()

        code, logs, report_df = run_pipeline(
            input_file=excel_path,
            sheet=sheet,
            cols=cols,
            token_file=token_file,
            max_words=int(max_words),
            threads_mejora=int(threads_mejora),
            threads_validacion=int(threads_validacion),
            max_topicos=int(max_topicos),
            max_contextos=int(max_contextos),
            export_spss=export_spss,
            progress_bar=progress_bar,
            status_box=status_box,
            eta_box=eta_box,
            report_box=report_box,
            step_progress_box=step_progress_box,
        )

        report_box.dataframe(report_df, use_container_width=True, hide_index=True)

        st.subheader("Logs")
        st.download_button(
            "Extraer log (.txt)",
            data=logs.encode("utf-8"),
            file_name="textana_log.txt",
            mime="text/plain",
        )
        render_copy_button(logs)
        st.text_area("Log copiable", value=logs if logs else "(sin salida)", height=log_height)

        if code == 0:
            status_box.success("Pipeline completado.")
            paso3 = OUTPUT_DIR / "Paso3_clasificacion.xlsx"
            if paso3.exists():
                st.session_state["result_excel_path"] = str(paso3)
                st.session_state["allow_graphs_from_run"] = True
                st.download_button(
                    "Descargar Paso3_clasificacion.xlsx",
                    data=paso3.read_bytes(),
                    file_name="Paso3_clasificacion.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        else:
            st.session_state["allow_graphs_from_run"] = False
            status_box.error(f"Pipeline finalizo con error (code={code}).")

    # Permite visualizar/modificar graficos aun sin volver a correr el pipeline.
    st.divider()
    st.header("Graficos en la plataforma")
    default_result_excel = OUTPUT_DIR / "Paso3_clasificacion.xlsx"

    up_col, down_col = st.columns([1.2, 1.0])
    with up_col:
        uploaded_result = st.file_uploader(
            "Cargar archivo de resultados para graficos",
            type=["xlsx"],
            key="graficos_result_upload",
            help="Carga un Excel de resultados (por ejemplo Paso3_clasificacion.xlsx) para editar y visualizar graficos.",
        )
        if uploaded_result is not None:
            try:
                loaded_target = save_uploaded_result_excel(bytes(uploaded_result.getbuffer()), stem="Paso3_clasificacion_cargado")
                st.session_state["result_excel_path"] = str(loaded_target)
                st.session_state["allow_graphs_from_upload"] = True
                st.success(f"Archivo cargado: {uploaded_result.name}")
            except PermissionError:
                st.error("No se pudo guardar el archivo cargado. Cierra Excel si tiene abierto un archivo de output e intenta de nuevo.")
            except Exception as exc:
                st.error(f"No se pudo guardar el archivo cargado: {exc}")

        uploaded_secure = st.file_uploader(
            "Cargar paquete seguro (.textana)",
            type=["textana"],
            key="graficos_secure_pkg_upload",
            help="Carga un paquete firmado para habilitar y editar graficos en la plataforma.",
        )
        if not secure_package_available():
            st.caption("Aviso: la validacion de paquetes cifrados requiere `cryptography` en este entorno.")
        if uploaded_secure is not None:
            ok, msg, payload_bytes, src_name, extra_files = validate_secure_textana_package(uploaded_secure.getvalue())
            if ok and payload_bytes is not None:
                try:
                    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(src_name or "resultado").stem).strip("._") or "resultado"
                    loaded_target = save_uploaded_result_excel(payload_bytes, stem=f"{safe_stem}_secure_cargado")
                    restore_viewer_extra_files(extra_files)
                    st.session_state["result_excel_path"] = str(loaded_target)
                    st.session_state["allow_graphs_from_upload"] = True
                    st.success(f"{msg} Archivo cargado: {Path(src_name or 'resultado.xlsx').name}")
                except PermissionError:
                    st.error("No se pudo guardar el paquete cargado. Cierra Excel si hay archivos de output abiertos e intenta de nuevo.")
                except Exception as exc:
                    st.error(f"No se pudo guardar el paquete cargado: {exc}")
            else:
                st.error(msg)

    result_excel = Path(st.session_state.get("result_excel_path", ""))
    graphs_enabled = bool(
        (st.session_state.get("allow_graphs_from_run", False) or st.session_state.get("allow_graphs_from_upload", False))
        and result_excel
        and result_excel.exists()
    )

    with down_col:
        if graphs_enabled:
            excel_complete, excel_issues = check_viewer_excel_completeness(result_excel)
            st.download_button(
                "Exportar archivo de graficos (.xlsx)",
                data=result_excel.read_bytes(),
                file_name=result_excel.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            if not excel_complete:
                st.warning("El archivo actual no contiene toda la estructura esperada para mostrar todos los graficos en el viewer:")
                for it in excel_issues:
                    st.caption(f"- {it}")
            if secure_package_available() and excel_complete:
                secure_pkg_bytes = build_secure_textana_package(
                    result_excel.read_bytes(),
                    source_name=result_excel.name,
                    extra_files=collect_viewer_extra_files(),
                )
                st.download_button(
                    "Exportar paquete seguro (.textana)",
                    data=secure_pkg_bytes,
                    file_name=f"{result_excel.stem}.textana",
                    mime="application/octet-stream",
                    use_container_width=True,
                )
            else:
                st.button("Exportar paquete seguro (.textana)", disabled=True, use_container_width=True)
        else:
            st.button("Exportar archivo de graficos (.xlsx)", disabled=True, use_container_width=True)
            st.button("Exportar paquete seguro (.textana)", disabled=True, use_container_width=True)

    if graphs_enabled:
        render_visual_dashboard(result_excel, selected_cross_vars_param=cross_vars_param)
    else:
        st.info("Los graficos se habilitan solo cuando: 1) ejecutas Textana completo con exito, o 2) cargas un Excel de resultados.")


if __name__ == "__main__":
    main()
