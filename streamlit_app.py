#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import hmac
import hashlib
import os
import re
import shutil
import subprocess
import sys
import time
import unicodedata
import zipfile
from datetime import datetime
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
import networkx as nx

try:
    import community as community_louvain  # python-louvain
except Exception:
    community_louvain = None

try:
    from pyvis.network import Network
except Exception:
    Network = None

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:
    Fernet = None
    InvalidToken = Exception


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
SESSION_ASSETS_ROOT = OUTPUT_DIR / "session_assets"
SESSION_WORK_ROOT = OUTPUT_DIR / "session_work"

STEP_LABELS = {
    "paso1_lectura.py": "Paso 1 - Lectura",
    "paso2_mejora.py": "Paso 2 - Mejora",
    "paso3_clasificacion.py": "Paso 3 - Clasificacion",
    "paso4_export_spss.py": "Paso 4 - Exportacion SPSS",
    "paso5_graficos.py": "Paso 5 - Graficos",
}


def _short_hash(text: str, size: int = 16) -> str:
    return hashlib.sha1(str(text).encode("utf-8", errors="ignore")).hexdigest()[:size]


def get_session_id() -> str:
    sid = str(st.session_state.get("session_id", "")).strip()
    if not sid:
        sid = f"sess_{_short_hash(str(time.time_ns()), size=20)}"
        st.session_state["session_id"] = sid
    return sid


def get_session_work_dir() -> Path:
    d = SESSION_WORK_ROOT / get_session_id()
    d.mkdir(parents=True, exist_ok=True)
    return d


def cleanup_temp_outputs_once(max_age_hours: int = 24) -> None:
    key = "_temp_cleanup_done"
    if st.session_state.get(key):
        return
    st.session_state[key] = True
    cutoff = time.time() - max_age_hours * 3600
    for base in [SESSION_WORK_ROOT, SESSION_ASSETS_ROOT]:
        if not base.exists():
            continue
        for p in base.iterdir():
            try:
                mtime = p.stat().st_mtime
            except Exception:
                continue
    # Limpia residuos antiguos en output de estrategias previas con timestamp.
    if OUTPUT_DIR.exists():
        legacy_patterns = ("_cargado_", "_viewer_")
        for fp in OUTPUT_DIR.glob("*.xlsx"):
            name = fp.name.lower()
            if not any(pat in name for pat in legacy_patterns):
                continue
            try:
                if fp.stat().st_mtime < cutoff:
                    fp.unlink(missing_ok=True)
            except Exception:
                continue
            if mtime >= cutoff:
                continue
            try:
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                elif p.is_file():
                    p.unlink(missing_ok=True)
            except Exception:
                continue


def set_active_assets_scope(scope_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(scope_id)).strip("._") or f"sess_{int(time.time())}"
    if st.session_state.get("active_assets_scope") == safe:
        assets_dir = SESSION_ASSETS_ROOT / safe
        assets_dir.mkdir(parents=True, exist_ok=True)
        return assets_dir
    st.session_state["active_assets_scope"] = safe
    assets_dir = SESSION_ASSETS_ROOT / safe
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


def set_active_assets_for_file(file_path: Path) -> Path:
    fp = Path(file_path)
    try:
        token = str(fp.resolve())
    except Exception:
        token = str(fp)
    # Scope estable por archivo (sin depender de mtime/tamano).
    scope = f"file_{_short_hash(token)}"
    return set_active_assets_scope(scope)


def get_active_assets_dir() -> Path:
    scope = str(st.session_state.get("active_assets_scope", "")).strip()
    if not scope:
        scope = f"sess_{_short_hash(str(time.time_ns()))}"
        st.session_state["active_assets_scope"] = scope
    assets_dir = SESSION_ASSETS_ROOT / scope
    assets_dir.mkdir(parents=True, exist_ok=True)
    return assets_dir


def clean_text_value(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return ""
    return s


def fix_mojibake_text(s: str) -> str:
    txt = "" if s is None else str(s)
    if not any(tok in txt for tok in ("Ã", "Â", "â€")):
        return txt
    try:
        fixed = txt.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        # Usa la version reparada solo si mejora claramente.
        if fixed and fixed.count("Ã") + fixed.count("Â") < txt.count("Ã") + txt.count("Â"):
            return fixed
    except Exception:
        pass
    return txt


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


def is_otros_label(text: str) -> bool:
    s = clean_text_value(text).lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s == "otros" or s.startswith("otros ")


def sort_items_otros_last(
    df: pd.DataFrame,
    label_col: str = "Item",
    freq_col: str = "Frecuencia",
    ascending_freq: bool = False,
) -> pd.DataFrame:
    if df.empty or label_col not in df.columns:
        return df
    out = df.copy()
    out["__is_otros"] = out[label_col].apply(is_otros_label)
    out = out.sort_values(
        by=["__is_otros", freq_col, label_col],
        ascending=[True, ascending_freq, True],
        kind="stable",
    )
    return out.drop(columns=["__is_otros"])


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


def _hsl_soft_color(group_id: int) -> str:
    hue = (int(group_id) * 50) % 360
    return f"hsl({hue}, 70%, 70%)"


def build_network_html_from_df(
    df_corr: pd.DataFrame,
    out_html_path: Path,
    max_topicos: int,
    min_relacion: int = 2,
    top_k_edges: int = 80,
    height_px: int = 700,
    max_nodes: int = 120,
    node_font_size: int = 14,
    node_size_scale: float = 1.2,
    node_size_max: int = 50,
    label_max_len: int = 120,
) -> tuple[bool, str]:
    if Network is None:
        return False, "No se pudo construir red: falta pyvis."
    if df_corr is None or df_corr.empty:
        return False, "No se pudo construir red: DataFrame vacio."

    topic_cols = [c for c in df_corr.columns if str(c).startswith("Topico_")]
    topic_cols = sorted(
        topic_cols,
        key=lambda x: int(str(x).split("_")[-1]) if str(x).split("_")[-1].isdigit() else str(x),
    )[: max(1, int(max_topicos))]
    if len(topic_cols) < 2:
        return False, "No se pudo construir red: se requieren al menos dos Topico_*."

    G = nx.Graph()
    # Firma de datos relevantes para evitar reconstrucciones innecesarias.
    rel_cols_for_sig = list(topic_cols)
    for c in df_corr.columns:
        if str(c).startswith("COD_Topico_"):
            rel_cols_for_sig.append(str(c))
    rel_cols_for_sig = [c for c in dict.fromkeys(rel_cols_for_sig) if c in df_corr.columns]
    try:
        sig_df = df_corr[rel_cols_for_sig].fillna("").astype(str)
        sig_raw = pd.util.hash_pandas_object(sig_df, index=True).values.tobytes()
        data_sig = hashlib.sha1(sig_raw).hexdigest()
    except Exception:
        data_sig = hashlib.sha1(str(df_corr.shape).encode("utf-8")).hexdigest()
    render_sig = _short_hash(
        f"{data_sig}|{max_topicos}|{min_relacion}|{top_k_edges}|{height_px}|"
        f"{max_nodes}|{node_font_size}|{node_size_scale}|{node_size_max}|{label_max_len}",
        size=24,
    )
    html_cache_key = "_network_html_cache"
    path_sig_key = f"_network_sig::{str(out_html_path)}"
    html_cache: dict[str, str] = st.session_state.setdefault(html_cache_key, {})
    if st.session_state.get(path_sig_key) == render_sig and out_html_path.exists():
        return True, "ok (cached)"

    # Mapa de contexto -> codigo (si existe en la base)
    code_map: dict[str, str] = {}
    cod_cols_by_idx: dict[str, str] = {}
    for c in df_corr.columns:
        c_str = str(c)
        if c_str.startswith("COD_Topico_"):
            idx = c_str.split("_")[-1]
            cod_cols_by_idx[idx] = c_str

    for _, row in df_corr.iterrows():
        vals: list[str] = []
        for c in topic_cols:
            v = clean_text_value(row.get(c, ""))
            if v:
                vals.append(v)
                idx = str(c).split("_")[-1]
                cod_col = cod_cols_by_idx.get(idx)
                if cod_col:
                    cod_raw = clean_text_value(row.get(cod_col, ""))
                    if cod_raw and v not in code_map:
                        code_map[v] = cod_raw
        vals = list(dict.fromkeys(vals))
        for i in range(len(vals)):
            for j in range(i + 1, len(vals)):
                a, b = sorted([vals[i], vals[j]])
                if G.has_edge(a, b):
                    G[a][b]["weight"] = int(G[a][b]["weight"]) + 1
                else:
                    G.add_edge(a, b, weight=1)

    if G.number_of_nodes() == 0:
        return False, "No se pudo construir red: sin nodos."

    # Filtra y conserva solo top-k aristas por peso.
    edges = sorted(G.edges(data=True), key=lambda e: int(e[2].get("weight", 1)), reverse=True)
    edges = [e for e in edges if int(e[2].get("weight", 1)) >= int(min_relacion)]
    edges = edges[: max(1, int(top_k_edges))]
    if not edges:
        return False, "No se pudo construir red: sin aristas con el filtro actual."

    Gf = nx.Graph()
    for u, v, d in edges:
        Gf.add_edge(u, v, weight=int(d.get("weight", 1)))

    # Limita cantidad de nodos por relevancia (fuerza total de conexiones).
    if int(max_nodes) > 0 and Gf.number_of_nodes() > int(max_nodes):
        node_strength = {
            n: sum(int(dd.get("weight", 1)) for _, _, dd in Gf.edges(n, data=True))
            for n in Gf.nodes()
        }
        keep = {
            n for n, _ in sorted(node_strength.items(), key=lambda kv: kv[1], reverse=True)[: int(max_nodes)]
        }
        Gf = Gf.subgraph(keep).copy()
        if Gf.number_of_edges() == 0:
            return False, "No se pudo construir red: sin aristas tras limitar nodos."

    try:
        if community_louvain is not None:
            partition = community_louvain.best_partition(Gf)
        else:
            partition = {n: 0 for n in Gf.nodes()}
    except Exception:
        partition = {n: 0 for n in Gf.nodes()}

    pos = nx.kamada_kawai_layout(Gf)

    net = Network(
        height=f"{int(height_px)}px",
        width="100%",
        bgcolor="white",
        font_color="black",
        directed=False,
        notebook=False,
    )
    net.toggle_physics(False)

    for node in Gf.nodes():
        freq = sum(int(d.get("weight", 1)) for _, _, d in Gf.edges(node, data=True))
        size = min(float(node_size_max), 5.0 + float(freq) * float(node_size_scale))
        group = int(partition.get(node, 0))
        color = _hsl_soft_color(group)
        x, y = pos[node]
        label_txt = node if len(node) <= int(label_max_len) else (node[: max(3, int(label_max_len) - 3)].rstrip() + "...")
        cod = code_map.get(node, "")
        label_default = f"{cod} - {label_txt}".strip(" -") if cod else label_txt
        title_txt = f"COD: {cod} - {node}" if cod else node
        net.add_node(
            node,
            label=label_default,
            title=title_txt,
            size=float(size),
            color=color,
            physics=False,
            x=float(x * 1000.0),
            y=float(y * 1000.0),
            font={"size": int(node_font_size), "face": "Verdana", "color": "black"},
            base_size=float(size),
            codigo=str(cod),
            nombre=node,
            grupo=group,
        )

    for u, v, d in Gf.edges(data=True):
        w = int(d.get("weight", 1))
        edge_group = int(partition.get(u, 0))
        net.add_edge(
            u,
            v,
            value=w,
            title=f"Relacion: {w}",
            color="rgba(120,140,160,0.24)",
            grupo=edge_group,
            smooth=False,
            physics=False,
        )

    net.set_options(
        """
        {
          "physics": {"enabled": false},
          "edges": {"smooth": false},
          "interaction": {
            "navigationButtons": true,
            "keyboard": true,
            "zoomView": true,
            "dragView": true
          },
          "nodes": {
            "font": {"size": %d, "face": "Verdana", "color": "black"}
          }
        }
        """ % int(node_font_size)
    )

    out_html_path.parent.mkdir(parents=True, exist_ok=True)
    html = html_cache.get(render_sig)
    if html is None:
        net.write_html(str(out_html_path), open_browser=False)
        # Inyecta controles legacy en el HTML para mantener el mismo UX de red.
        html = out_html_path.read_text(encoding="utf-8", errors="replace")
    panel = """
    <style>
      .netui-wrap {
        padding: 10px 12px;
        background: #f7f7f5;
        border-bottom: 1px solid #e7e4df;
        font-family: Verdana, Arial, sans-serif;
        color: #364252;
      }
      .netui-row {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 8px 10px;
        margin-bottom: 8px;
      }
      .netui-label { color: #5c6674; font-size: 12px; font-weight: 600; }
      .netui-val { color: #6b7280; font-size: 12px; min-width: 28px; display: inline-block; }
      .netui-btn {
        height: 30px; padding: 0 10px; border-radius: 8px; border: 1px solid #dfd8d0;
        background: #fff; color: #364252; font-size: 12px; cursor: pointer;
      }
      .netui-btn:hover { background: #f6f2ef; }
      .netui-btn-primary {
        border-color: #8a4b52; background: #a65a63; color: #fff;
      }
      .netui-btn-primary:hover { background: #8f4f57; }
      .netui-input, .netui-select {
        height: 30px; border: 1px solid #dfd8d0; border-radius: 8px;
        padding: 0 8px; font-size: 12px; color: #364252; background: #fff;
      }
      .netui-range { accent-color: #a65a63; }
    </style>
    <div class="netui-wrap">
      <div class="netui-row">
        <button class="netui-btn netui-btn-primary" onclick="resetLayout()">Reiniciar layout</button>
        <span class="netui-label">Peso minimo:</span>
        <input class="netui-range" type="range" id="pesoSlider" min="1" max="30" value="10" step="1" oninput="updateEdgeThreshold(this.value)">
        <span id="pesoValue" class="netui-val">10</span>
        <span class="netui-label">Top-K aristas:</span>
        <input class="netui-range" type="range" id="topKSlider" min="0" max="200" value="30" step="1" oninput="updateTopK(this.value)">
        <span id="topKValue" class="netui-val">30</span>
        <span class="netui-label">Paleta:</span>
        <select class="netui-select" id="paletteSelect" onchange="applyPalette(this.value)">
          <option value="sankey">Sankey sobria</option>
          <option value="steel">Acero suave</option>
          <option value="forest">Bosque suave</option>
          <option value="pastel_blue">Pastel azul</option>
          <option value="pastel_mix">Pastel mixto</option>
        </select>
        <span class="netui-label">Buscar nodo:</span>
        <input class="netui-input" type="text" id="searchBox" oninput="searchNode()" placeholder="Escribe nombre o codigo">
        <button class="netui-btn" onclick="applyFocusFromSearch()">Foco</button>
        <button class="netui-btn" onclick="clearFocus()">Limpiar foco</button>
      </div>
      <div class="netui-row">
        <button class="netui-btn" onclick="toggleLabels()">Mostrar/ocultar etiquetas</button>
        <span class="netui-label">Modo etiqueta:</span>
        <select class="netui-select" id="labelModeSelect" onchange="updateLabelMode(this.value)">
          <option value="code">Codigo</option>
          <option value="name">Nombre</option>
          <option value="both" selected>Codigo + Nombre</option>
        </select>
        <span class="netui-label">Tamano etiqueta:</span>
        <input class="netui-range" type="range" id="labelSizeSlider" min="8" max="40" value="37" oninput="updateLabelSize(this.value)">
        <span id="labelSizeValue" class="netui-val">37</span>
        <span class="netui-label">Escala esferas:</span>
        <input class="netui-range" type="range" id="nodeScaleSlider" min="50" max="250" value="100" step="5" oninput="updateNodeScale(this.value)">
        <span id="nodeScaleValue" class="netui-val">100%</span>
        <span class="netui-label">Tam max esfera:</span>
        <input class="netui-range" type="range" id="nodeCapSlider" min="20" max="120" value="80" step="1" oninput="updateNodeCap(this.value)">
        <span id="nodeCapValue" class="netui-val">80</span>
        <button class="netui-btn" onclick="exportPNG()">Exportar PNG</button>
      </div>
    </div>
    """
    js_extra = """
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <script>
    let labelsVisible = true;
    let labelMode = "both";
    let minWeight = 10;
    let topKEdges = 30;
    let focusedNode = null;
    let nodeScalePct = 100;
    let nodeSizeCap = 80;
    let currentPalette = "sankey";
    const mutedNodeColor = { background: "rgba(208,216,226,0.8)", border: "rgba(157,169,183,1)" };
    const mutedEdgeColor = "rgba(176,184,193,0.28)";
    const defaultNodeColor = {};
    const defaultNodeFontColor = {};
    const defaultNodeSize = {};
    const defaultEdgeColor = {};
    const PALETTES = {
      sankey: ["#1F4E79", "#2F6B92", "#4E7D6B", "#7A6F9B", "#8A6D5A", "#3D5A80", "#5A7D5A", "#6C7A89"],
      steel:  ["#2B4C6F", "#3F6A89", "#5B7C8F", "#6A7E95", "#7B6E88", "#6E7D86", "#4C6B7D", "#8094A5"],
      forest: ["#3E5F6A", "#4E7D6B", "#607D6A", "#6D7F7A", "#587089", "#6D7A8E", "#7A6F9B", "#5E7B85"],
      pastel_blue: ["#8FB7D6", "#9CC2DE", "#A9CCE5", "#B7D6EC", "#9FBFDC", "#AECBE2", "#BBD5E8", "#C7DFEE"],
      pastel_mix: ["#AFC8E9", "#C5B8E8", "#B9D9C7", "#E3C8A8", "#BFD6DC", "#D8C7E5", "#CFE3D2", "#E5D8C6"]
    };

    function hexToRgba(hex, alpha) {
      const h = String(hex || "").replace("#", "");
      if (h.length !== 6) return "rgba(120,140,160," + String(alpha) + ")";
      const r = parseInt(h.slice(0, 2), 16);
      const g = parseInt(h.slice(2, 4), 16);
      const b = parseInt(h.slice(4, 6), 16);
      return "rgba(" + r + "," + g + "," + b + "," + String(alpha) + ")";
    }

    function darkenHex(hex, factor) {
      const h = String(hex || "").replace("#", "");
      if (h.length !== 6) return "#5b6f82";
      let r = parseInt(h.slice(0, 2), 16);
      let g = parseInt(h.slice(2, 4), 16);
      let b = parseInt(h.slice(4, 6), 16);
      r = Math.max(0, Math.min(255, Math.round(r * (1 - factor))));
      g = Math.max(0, Math.min(255, Math.round(g * (1 - factor))));
      b = Math.max(0, Math.min(255, Math.round(b * (1 - factor))));
      return "#" + r.toString(16).padStart(2, "0") + g.toString(16).padStart(2, "0") + b.toString(16).padStart(2, "0");
    }

    function getPaletteColor(groupId) {
      const arr = PALETTES[currentPalette] || PALETTES.sankey;
      const gi = Number(groupId || 0);
      return arr[Math.abs(gi) % arr.length];
    }

    function preserveView(fn) {
      const pos = network.getViewPosition();
      const scale = network.getScale();
      fn();
      network.moveTo({position: pos, scale: scale, animation: false});
    }

    function applyPalette(name) {
      currentPalette = String(name || "sankey");
      preserveView(function() {
        const nodes = network.body.data.nodes;
        const edges = network.body.data.edges;
        nodes.getIds().forEach(function(id) {
          const n = nodes.get(id);
          const base = getPaletteColor(n.grupo || 0);
          const border = darkenHex(base, 0.24);
          n.color = {
            background: hexToRgba(base, 0.90),
            border: hexToRgba(border, 0.98),
            highlight: { background: hexToRgba(base, 0.98), border: hexToRgba(border, 1.0) },
            hover: { background: hexToRgba(base, 0.98), border: hexToRgba(border, 1.0) }
          };
          nodes.update(n);
          defaultNodeColor[id] = n.color;
        });
        edges.getIds().forEach(function(id) {
          const e = edges.get(id);
          const base = getPaletteColor(e.grupo || 0);
          e.color = {
            color: hexToRgba(base, 0.24),
            highlight: hexToRgba(base, 0.42),
            hover: hexToRgba(base, 0.52)
          };
          edges.update(e);
          defaultEdgeColor[id] = e.color;
        });
      });
      applyGraphFilters();
    }

    function getNodeLabel(item) {
      const code = item.codigo || "";
      const name = item.nombre || "";
      if (!labelsVisible) return "";
      if (labelMode === "code") return code || name;
      if (labelMode === "name") return name || code;
      return (code && name) ? (code + " - " + name) : (code || name);
    }

    function applyLabels() {
      preserveView(function() {
        const nodes = network.body.data.nodes;
        nodes.getIds().forEach(function(id) {
          const item = nodes.get(id);
          item.label = getNodeLabel(item);
          nodes.update(item);
        });
      });
    }

    function toggleLabels() { labelsVisible = !labelsVisible; applyLabels(); }
    function updateLabelMode(mode) { labelMode = mode; labelsVisible = true; applyLabels(); }

    function updateLabelSize(size) {
      document.getElementById("labelSizeValue").innerText = size;
      preserveView(function() {
        const nodes = network.body.data.nodes;
        nodes.getIds().forEach(function(id) {
          const item = nodes.get(id);
          item.font = Object.assign({}, item.font || {}, { size: Number(size) });
          nodes.update(item);
        });
      });
    }

    function updateNodeScale(val) {
      nodeScalePct = Number(val);
      document.getElementById("nodeScaleValue").innerText = String(nodeScalePct) + "%";
      updateNodeSizes();
    }

    function updateNodeCap(val) {
      nodeSizeCap = Number(val);
      document.getElementById("nodeCapValue").innerText = String(nodeSizeCap);
      updateNodeSizes();
    }

    function updateNodeSizes() {
      preserveView(function() {
        const nodes = network.body.data.nodes;
        const scale = nodeScalePct / 100.0;
        const minSize = 10;
        nodes.getIds().forEach(function(id) {
          const item = nodes.get(id);
          const base = Number(defaultNodeSize[id] || item.base_size || item.size || 20);
          let adjusted = base * scale;
          adjusted = Math.max(minSize, Math.min(nodeSizeCap, adjusted));
          item.size = adjusted;
          nodes.update(item);
        });
      });
    }

    function getFilteredEdgeIds() {
      const edges = network.body.data.edges;
      let byWeight = edges.get().filter(function(e) { return Number(e.value || 0) >= Number(minWeight); });
      byWeight.sort(function(a, b) { return Number(b.value || 0) - Number(a.value || 0); });
      if (Number(topKEdges) > 0) byWeight = byWeight.slice(0, Number(topKEdges));
      return new Set(byWeight.map(function(e) { return e.id; }));
    }

    function applyGraphFilters() {
      preserveView(function() {
        const nodes = network.body.data.nodes;
        const edges = network.body.data.edges;
        const keepEdgeIds = getFilteredEdgeIds();
        const activeNodeIds = new Set();
        const focusNeighborhood = new Set();

        if (focusedNode !== null) {
          focusNeighborhood.add(focusedNode);
          edges.get().forEach(function(e) {
            if (!keepEdgeIds.has(e.id)) return;
            if (e.from === focusedNode) focusNeighborhood.add(e.to);
            if (e.to === focusedNode) focusNeighborhood.add(e.from);
          });
        }

        edges.getIds().forEach(function(id) {
          const e = edges.get(id);
          const passBase = keepEdgeIds.has(id);
          const passFocus = (focusedNode === null) || (e.from === focusedNode || e.to === focusedNode);
          const visible = passBase && passFocus;
          e.hidden = !visible;
          e.color = visible ? (defaultEdgeColor[id] || undefined) : mutedEdgeColor;
          if (visible) {
            activeNodeIds.add(e.from);
            activeNodeIds.add(e.to);
          }
          edges.update(e);
        });

        nodes.getIds().forEach(function(id) {
          const n = nodes.get(id);
          const hasVisibleUnion = activeNodeIds.has(id);
          const isFocused = (focusedNode !== null && id === focusedNode);
          const showNode = hasVisibleUnion || isFocused;
          n.hidden = !showNode;
          if (!showNode) { nodes.update(n); return; }
          if (focusedNode === null || focusNeighborhood.has(id) || isFocused) {
            n.color = defaultNodeColor[id] || n.color;
            n.font = Object.assign({}, n.font || {}, { color: defaultNodeFontColor[id] || "black" });
          } else {
            n.color = mutedNodeColor;
            n.font = Object.assign({}, n.font || {}, { color: "#9fa9b4" });
          }
          nodes.update(n);
        });
      });
    }

    function updateEdgeThreshold(val) {
      document.getElementById("pesoValue").innerText = val;
      minWeight = Number(val);
      applyGraphFilters();
    }

    function updateTopK(val) {
      topKEdges = Number(val);
      document.getElementById("topKValue").innerText = topKEdges === 0 ? "todos" : String(topKEdges);
      applyGraphFilters();
    }

    function findByQuery() {
      const q = (document.getElementById("searchBox").value || "").toLowerCase().trim();
      if (!q) return null;
      const nodes = network.body.data.nodes;
      let found = null;
      nodes.getIds().forEach(function(id) {
        const item = nodes.get(id);
        const haystack = ((item.title || "") + " " + (item.codigo || "") + " " + (item.nombre || "")).toLowerCase();
        if (haystack.includes(q)) found = id;
      });
      return found;
    }

    function searchNode() {
      const found = findByQuery();
      if (found !== null) network.focus(found, {scale: 1.4, animation: false});
    }

    function applyFocusFromSearch() {
      const found = findByQuery();
      if (found !== null) {
        focusedNode = found;
        applyGraphFilters();
        network.focus(found, {scale: 1.5, animation: false});
      }
    }

    function clearFocus() { focusedNode = null; applyGraphFilters(); }
    function resetLayout() { network.moveTo({position: {x: 0, y: 0}, scale: 1}); }

    function exportPNG() {
      const container = document.getElementById("mynetwork");
      html2canvas(container).then(function(canvas) {
        const link = document.createElement("a");
        link.download = "red_textana.png";
        link.href = canvas.toDataURL();
        link.click();
      });
    }

    document.addEventListener("DOMContentLoaded", function() {
      const nodes = network.body.data.nodes;
      const edges = network.body.data.edges;
      nodes.getIds().forEach(function(id) {
        const item = nodes.get(id);
        defaultNodeColor[id] = item.color;
        defaultNodeFontColor[id] = (item.font && item.font.color) ? item.font.color : "black";
        defaultNodeSize[id] = Number(item.size || item.base_size || 20);
      });
      edges.getIds().forEach(function(id) {
        const e = edges.get(id);
        defaultEdgeColor[id] = e.color;
      });
      const paletteSel = document.getElementById("paletteSelect");
      if (paletteSel) {
        paletteSel.value = currentPalette;
      }
      applyPalette(currentPalette);
      applyLabels();
      updateLabelSize(37);
      updateNodeSizes();
      applyGraphFilters();
    });
    </script>
    """

    if 'id="pesoSlider"' not in html and "<body>" in html:
        html = html.replace("<body>", "<body>\n" + panel, 1)
    if "function updateEdgeThreshold" not in html:
        html = html.replace("</body>", js_extra + "\n</body>")
    html_cache[render_sig] = html
    # Limita cache en memoria para no crecer indefinidamente.
    if len(html_cache) > 20:
        for k in list(html_cache.keys())[:-20]:
            html_cache.pop(k, None)
    st.session_state[html_cache_key] = html_cache

    html_bytes = html.encode("utf-8")
    write_needed = True
    if out_html_path.exists():
        try:
            write_needed = out_html_path.read_bytes() != html_bytes
        except Exception:
            write_needed = True
    if write_needed:
        out_html_path.write_bytes(html_bytes)
    st.session_state[path_sig_key] = render_sig
    return True, "ok"


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
        # Acepta secreto en texto plano o en Base64 URL-safe (recomendado para Streamlit Secrets).
        try:
            padded = env_key + ("=" * ((4 - len(env_key) % 4) % 4))
            decoded = base64.urlsafe_b64decode(padded.encode("utf-8"))
            if len(decoded) >= 16:
                return decoded
        except Exception:
            pass
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
          /* -------- Modulo Red Interactiva (estilo unificado) -------- */
          .net-module-shell {
            background: #f7f7f5;
            border: 1px solid #e7e4df;
            border-radius: 12px;
            padding: 12px;
            margin-top: 6px;
            margin-bottom: 10px;
          }
          .net-module-title {
            color: #364252;
            font-weight: 600;
            font-size: 1rem;
            margin-bottom: 2px;
          }
          .net-module-subtitle {
            color: #6b7280;
            font-size: 0.86rem;
            margin-bottom: 10px;
          }
          .net-group-title {
            color: #4a5564;
            font-weight: 600;
            font-size: 0.9rem;
            margin-bottom: 4px;
          }
          .net-group-caption {
            color: #7a8594;
            font-size: 0.78rem;
            margin-bottom: 8px;
          }
          .net-canvas-shell {
            background: #ffffff;
            border: 1px solid #e7e4df;
            border-radius: 12px;
            padding: 10px;
          }
          .net-mini-muted {
            color: #6b7280;
            font-size: 0.8rem;
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


def save_uploaded_result_excel(uploaded_bytes: bytes, stem: str = "Paso3_clasificacion_cargado", source_name: str = "") -> Path:
    SESSION_WORK_ROOT.mkdir(parents=True, exist_ok=True)
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", str(stem)).strip("._") or "resultado_cargado"
    src = clean_text_value(source_name) or safe_stem
    src_hash = _short_hash(src, size=14)
    target_dir = get_session_work_dir() / f"{safe_stem}_{src_hash}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "current_result.xlsx"
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


def collect_viewer_extra_files(assets_dir: Path | None = None) -> dict[str, bytes]:
    extras: dict[str, bytes] = {}
    base_dir = Path(assets_dir) if assets_dir is not None else get_active_assets_dir()
    if base_dir.exists():
        for fp in base_dir.rglob("*"):
            if not fp.is_file():
                continue
            rel = fp.relative_to(base_dir).as_posix()
            try:
                extras[rel] = fp.read_bytes()
            except Exception:
                continue
    return extras


def collect_original_graph_assets(assets_dir: Path | None = None) -> dict[str, bytes]:
    """Recolecta todos los graficos originales disponibles para exportacion."""
    assets: dict[str, bytes] = {}

    def _normalize_rel_name(rel_name: str) -> str:
        rel = str(rel_name).replace("\\", "/")
        # Evita duplicar la red con dos nombres distintos.
        if rel.lower().endswith("graficos/red_interactiva.html"):
            return "graficos/07_red_interactiva.html"
        return rel

    # 1) Todo lo que exista dentro de la carpeta de assets de la sesion/archivo actual.
    base_dir = Path(assets_dir) if assets_dir is not None else get_active_assets_dir()
    graficos_dir = base_dir / "graficos"
    if graficos_dir.exists():
        for fp in graficos_dir.rglob("*"):
            if not fp.is_file():
                continue
            if fp.suffix.lower() not in {".png", ".html", ".svg", ".jpg", ".jpeg", ".webp", ".json"}:
                continue
            try:
                rel_name = _normalize_rel_name(fp.relative_to(base_dir).as_posix())
                assets[f"original/{rel_name}"] = fp.read_bytes()
            except Exception:
                continue

    return assets


def restore_viewer_extra_files(extra_files: dict[str, bytes], assets_dir: Path | None = None) -> None:
    base_dir = Path(assets_dir) if assets_dir is not None else get_active_assets_dir()
    base_dir.mkdir(parents=True, exist_ok=True)
    for rel_path, content in extra_files.items():
        rel_norm = str(rel_path).replace("\\", "/").lstrip("/")
        if ".." in rel_norm.split("/"):
            continue
        target = base_dir / rel_norm
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


@st.cache_data(show_spinner=False)
def build_all_wordcloud_assets(
    result_excel: str,
    file_version: int,
    max_topicos: int,
    max_words: int,
    bg: str,
    wc_height: int,
) -> dict[str, bytes]:
    _ = file_version  # invalida cache cuando cambia el archivo
    df_corr = pd.read_excel(result_excel, sheet_name="clasificacion_sin_incorrectos", engine="openpyxl")
    text_col = "Texto.Mejorado" if "Texto.Mejorado" in df_corr.columns else "Texto.Original"
    topic_cols = [c for c in df_corr.columns if c.startswith("Topico_")]
    topic_cols = sorted(topic_cols, key=lambda x: int(x.split("_")[-1]) if x.split("_")[-1].isdigit() else x)
    if not topic_cols:
        return {}

    stop = set(STOPWORDS)
    stop.update(["la", "el", "de", "que", "y", "en", "por", "con", "los", "las", "un", "una"])

    vals = []
    for c in topic_cols[:max_topicos]:
        vals.extend(df_corr[c].apply(clean_text_value).tolist())
    topic_options = [v for v in dict.fromkeys(vals) if v]

    out: dict[str, bytes] = {}
    for topic_name in topic_options:
        mask_all = False
        for col in topic_cols[:max_topicos]:
            mask_all = mask_all | (df_corr[col].apply(clean_text_value) == topic_name)
        textos_all = df_corr.loc[mask_all, text_col].dropna().astype(str).tolist()
        if not textos_all:
            continue
        wc_all = WordCloud(
            width=1200,
            height=wc_height,
            background_color=bg,
            max_words=max_words,
            stopwords=stop,
        ).generate(" ".join(textos_all))
        buf_all = BytesIO()
        Image.fromarray(wc_all.to_array()).save(buf_all, format="PNG")
        topic_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(topic_name)).strip("._") or "item"
        out[f"nubes/04_nube_{topic_slug}.png"] = buf_all.getvalue()
    return out


def render_visual_dashboard(
    result_excel: Path,
    key_prefix: str = "viz",
    selected_cross_vars_param: list[str] | None = None,
    show_cross_section: bool = True,
    assets_dir: Path | None = None,
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

    # Reinicia controles visuales al cambiar de archivo para evitar "arrastre"
    # de estados previos que hace ver graficos distintos y marca falsos "editados".
    controls_ver_key = f"{key_prefix}_controls_filever"
    if st.session_state.get(controls_ver_key) != file_version:
        drop_keys = [
            k for k in st.session_state.keys()
            if str(k).startswith(f"{key_prefix}_")
            and not str(k).startswith(f"{key_prefix}_original_snapshot_")
            and k != controls_ver_key
        ]
        for k in drop_keys:
            st.session_state.pop(k, None)
        st.session_state[controls_ver_key] = file_version

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
    if "COD_Topico" in df_ldc.columns:
        mask_otros = df_ldc["Item"].apply(is_otros_label)
        if mask_otros.any():
            df_ldc.loc[mask_otros, "COD_Topico"] = 9999

    st.subheader("Visualizador interactivo")
    export_assets: dict[str, bytes] = {}
    session_assets_dir = Path(assets_dir) if assets_dir is not None else get_active_assets_dir()
    session_assets_dir.mkdir(parents=True, exist_ok=True)

    def save_session_asset(rel_path: str, data: bytes | None) -> None:
        if not data:
            return
        rel_norm = str(rel_path).replace("\\", "/").lstrip("/")
        if ".." in rel_norm.split("/"):
            return
        target = session_assets_dir / rel_norm
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    # Snapshot "original": captura lo visible al abrir (primer render por archivo).
    snapshot_ver_key = f"{key_prefix}_original_snapshot_filever"
    snapshot_assets_key = f"{key_prefix}_original_snapshot_assets"
    if st.session_state.get(snapshot_ver_key) != file_version:
        st.session_state[snapshot_ver_key] = file_version
        st.session_state[snapshot_assets_key] = {}
    snapshot_assets: dict[str, bytes] = dict(st.session_state.get(snapshot_assets_key, {}))

    def remember_original(asset_name: str, asset_bytes: bytes | None) -> None:
        if not asset_bytes:
            return
        original_name = f"original/graficos/{asset_name}"
        if original_name not in snapshot_assets:
            snapshot_assets[original_name] = asset_bytes
        save_session_asset(f"graficos/{asset_name}", asset_bytes)

    def harmonize_plotly_layout(fig, stem: str) -> None:
        # Mantiene margenes/etiquetas consistentes entre Streamlit y HTML exportado.
        if stem == "01_barras":
            # Ajusta margenes segun orientacion real de barras.
            bar_orientation = "v"
            try:
                if fig.data and getattr(fig.data[0], "orientation", None):
                    bar_orientation = str(fig.data[0].orientation).lower()
            except Exception:
                bar_orientation = "v"
            if bar_orientation == "h":
                fig.update_layout(margin=dict(l=260, r=40, t=60, b=60))
            else:
                fig.update_layout(margin=dict(l=70, r=40, t=60, b=220))
            fig.update_xaxes(automargin=True)
            fig.update_yaxes(automargin=True)
        elif stem == "02_pareto":
            fig.update_layout(margin=dict(l=80, r=90, t=60, b=210))
            fig.update_xaxes(automargin=True)
            fig.update_yaxes(automargin=True)
            if "yaxis2" in fig.layout:
                fig.layout.yaxis2.automargin = True
        elif stem.startswith("05_heatmap"):
            fig.update_layout(margin=dict(l=230, r=40, t=60, b=90))
            fig.update_xaxes(automargin=True)
            fig.update_yaxes(automargin=True)

    def add_plotly_exports(fig, edited_stem: str, edited: bool = True) -> None:
        export_fig = go.Figure(fig)
        harmonize_plotly_layout(export_fig, edited_stem)
        if export_fig.layout.height is None:
            export_fig.update_layout(height=620)
        export_fig.update_layout(autosize=False, width=1280)
        # Siempre exporta HTML para no depender de Kaleido.
        try:
            html_bytes = export_fig.to_html(
                include_plotlyjs="cdn",
                full_html=True,
                config={"responsive": False},
            ).encode("utf-8")
            if edited:
                export_assets[f"editado/graficos/{edited_stem}.html"] = html_bytes
            remember_original(f"{edited_stem}.html", html_bytes)
            save_session_asset(f"graficos/{edited_stem}.html", html_bytes)
        except Exception:
            pass
        # Si Kaleido esta disponible, tambien agrega PNG.
        png_bytes = fig_to_png_bytes(export_fig)
        if png_bytes:
            if edited:
                export_assets[f"editado/graficos/{edited_stem}.png"] = png_bytes
            remember_original(f"{edited_stem}.png", png_bytes)
            save_session_asset(f"graficos/{edited_stem}.png", png_bytes)

    c1, c2, c3 = st.columns(3)
    with c1:
        default_top_n = min(15, max(3, len(df_ldc)))
        top_n = st.slider(f"Top N {item_label.lower()}s", min_value=3, max_value=min(50, max(3, len(df_ldc))), value=default_top_n)
    with c2:
        min_freq = st.number_input("Frecuencia minima", min_value=1, value=1)
    with c3:
        palette = st.selectbox("Paleta", options=["Blues", "Viridis", "Cividis", "Plasma", "Magma", "Turbo"], index=0)
    global_has_edits = (top_n != default_top_n) or (int(min_freq) != 1) or (palette != "Blues")

    debug_mode = st.checkbox(
        "Modo diagnostico (debug)",
        value=False,
        key=f"{key_prefix}_debug_mode",
        help="Muestra logs tecnicos del render para detectar por que un control no aplica.",
    )
    debug_log_key = f"{key_prefix}_debug_logs"
    if debug_log_key not in st.session_state:
        st.session_state[debug_log_key] = []

    def dbg(msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(f"[VIZ-DEBUG] {line}", flush=True)
        if debug_mode:
            logs = st.session_state.get(debug_log_key, [])
            logs.append(line)
            if len(logs) > 300:
                logs = logs[-300:]
            st.session_state[debug_log_key] = logs

    graph_options = ["Barras", "Pareto", "Treemap", "Nube de palabras", "Heatmap", "Sankey"]
    graph_options.append("Red interactiva")
    viz_types = graph_options
    st.caption("Visualizacion completa: se muestran todos los graficos disponibles.")
    focus_only_one = st.checkbox(
        "Editar solo un grafico a la vez",
        value=False,
        key=f"{key_prefix}_focus_only_one",
        help="Cuando esta activo, solo se procesa y redibuja el grafico seleccionado.",
    )
    active_graph = st.selectbox(
        "Grafico activo",
        options=graph_options,
        index=0,
        key=f"{key_prefix}_active_graph",
        disabled=not focus_only_one,
    )
    if focus_only_one:
        st.caption(f"Modo enfocado activo: mostrando solo `{active_graph}`")
    dbg(
        f"file_version={file_version} top_n={top_n} min_freq={int(min_freq)} palette={palette} "
        f"focus_only_one={focus_only_one} active_graph={active_graph} options={graph_options}"
    )

    def should_render_graph(graph_name: str) -> bool:
        if graph_name not in viz_types:
            dbg(f"render_check {graph_name}=False (not in viz_types)")
            return False
        if not focus_only_one:
            dbg(f"render_check {graph_name}=True (focus_only_one off)")
            return True
        decision = active_graph == graph_name
        dbg(f"render_check {graph_name}={decision} (focus_only_one on, active_graph={active_graph})")
        return decision

    df_plot = df_ldc.copy()
    df_plot = df_plot[df_plot["Frecuencia"] >= min_freq]
    # Consolidar por contexto para evitar duplicados al venir Topico+Contexto en LDC.
    df_plot = (
        df_plot.groupby("Item", as_index=False)["Frecuencia"]
        .sum()
    )
    df_plot = sort_items_otros_last(df_plot, label_col="Item", freq_col="Frecuencia", ascending_freq=False).head(top_n)
    total_plot = df_plot["Frecuencia"].sum()
    if total_plot > 0:
        df_plot["Porcentaje"] = df_plot["Frecuencia"] / total_plot * 100
    else:
        df_plot["Porcentaje"] = 0.0

    if df_plot.empty:
        st.warning("No hay datos para graficar con esos filtros.")

    if should_render_graph("Barras") and not df_plot.empty:
        section_barras = st.expander("Seccion: Barras", expanded=True)
        with section_barras:
            with st.form(key=f"{key_prefix}_bar_form", border=False):
                bc1, bc2, bc3 = st.columns(3)
                with bc1:
                    orient = st.selectbox("Orientacion", options=["Horizontal", "Vertical"], index=0, key=f"{key_prefix}_bar_orient")
                with bc2:
                    bar_height = st.slider("Alto (px)", min_value=300, max_value=1000, value=520, step=10, key=f"{key_prefix}_bar_height")
                with bc3:
                    show_values = st.checkbox("Mostrar valores", value=True, key=f"{key_prefix}_bar_values")
                st.form_submit_button("Aplicar barras")

        orient_mode = "h" if str(orient).strip().lower().startswith("h") else "v"
        if orient_mode == "h":
            bar_df_h = sort_items_otros_last(df_plot, label_col="Item", freq_col="Frecuencia", ascending_freq=True)
            fig_bar = px.bar(
                bar_df_h,
                x="Frecuencia",
                y="Item",
                orientation="h",
                color="Frecuencia",
                color_continuous_scale=palette,
                title=f"Frecuencia por {item_label.lower()}",
            )
        else:
            fig_bar = px.bar(
                df_plot,
                x="Item",
                y="Frecuencia",
                orientation="v",
                color="Frecuencia",
                color_continuous_scale=palette,
                title=f"Frecuencia por {item_label.lower()}",
            )
            fig_bar.update_xaxes(tickangle=45)
        fig_bar.update_layout(height=bar_height)
        # Fuerza orientacion del trace para evitar inferencias inconsistentes de Plotly.
        fig_bar.update_traces(orientation=orient_mode)
        fig_bar.update_traces(
            customdata=df_plot[["Porcentaje"]].values if orient_mode == "v" else bar_df_h[["Porcentaje"]].values,
            hovertemplate="%{y}<br>Frecuencia: %{x}<br>Porcentaje: %{customdata[0]:.0f}%<extra></extra>" if orient_mode == "h" else "%{x}<br>Frecuencia: %{y}<br>Porcentaje: %{customdata[0]:.0f}%<extra></extra>",
        )
        if show_values:
            fig_bar.update_traces(
                textposition="outside",
                texttemplate="%{customdata[0]:.0f}%",
                cliponaxis=False,
            )
        harmonize_plotly_layout(fig_bar, "01_barras")
        section_barras.plotly_chart(fig_bar, use_container_width=True)
        bar_edited = global_has_edits or (orient != "Horizontal") or (bar_height != 520) or (show_values is not True)
        add_plotly_exports(fig_bar, "01_barras", edited=bar_edited)
        dbg(
            f"barras rendered orient={orient_mode} height={bar_height} show_values={show_values} "
            f"rows={len(df_plot)} edited={bar_edited}"
        )

    if should_render_graph("Pareto") and not df_plot.empty:
        try:
            section_pareto = st.expander("Seccion: Pareto", expanded=False)
            with section_pareto:
                with st.form(key=f"{key_prefix}_par_form", border=False):
                    pc1, pc2, pc3 = st.columns(3)
                    with pc1:
                        pareto_height = st.slider("Alto Pareto (px)", min_value=300, max_value=1000, value=520, step=10, key=f"{key_prefix}_par_height")
                    with pc2:
                        pareto_target = st.slider("Linea objetivo (%)", min_value=50, max_value=99, value=80, step=1, key=f"{key_prefix}_par_target")
                    with pc3:
                        show_markers = st.checkbox("Marcadores linea", value=True, key=f"{key_prefix}_par_markers")
                    st.form_submit_button("Aplicar Pareto")

            pareto_df = sort_items_otros_last(df_plot, label_col="Item", freq_col="Frecuencia", ascending_freq=False).copy()
            pareto_df["Porcentaje"] = pareto_df["Frecuencia"] / pareto_df["Frecuencia"].sum() * 100
            pareto_df["AcumuladoPct"] = pareto_df["Porcentaje"].cumsum()
            fig_pareto = px.bar(
                pareto_df,
                x="Item",
                y="Porcentaje",
                title=f"Pareto de {item_label.lower()}s",
            )
            # Colores fijos para que portal y exportacion salgan consistentes.
            fig_pareto.update_traces(marker_color="#1f77b4", selector=dict(type="bar"))
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
                line=dict(color="#6baed6", width=2),
                marker=dict(color="#6baed6", size=6),
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
            harmonize_plotly_layout(fig_pareto, "02_pareto")
            section_pareto.plotly_chart(fig_pareto, use_container_width=True)
            pareto_edited = global_has_edits or (pareto_height != 520) or (pareto_target != 80) or (show_markers is not True)
            add_plotly_exports(fig_pareto, "02_pareto", edited=pareto_edited)
            dbg(
                f"pareto rendered height={pareto_height} target={pareto_target} "
                f"markers={show_markers} rows={len(pareto_df)} edited={pareto_edited}"
            )
        except Exception as exc:
            st.error(f"No se pudo renderizar Pareto: {exc}")

    if should_render_graph("Treemap") and not df_plot.empty:
        report_theme = get_shared_report_theme()
        section_treemap = st.expander("Seccion: Treemap", expanded=False)
        with section_treemap:
            with st.form(key=f"{key_prefix}_tree_form", border=False):
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
                st.form_submit_button("Aplicar Treemap")

        text_map = {
            "Etiqueta": "label",
            "Etiqueta + valor": "label+value",
            "Etiqueta + % padre": "label+percent parent",
            "Etiqueta + % total": "label+percent root",
            "Completo": "label+value+percent parent+percent root",
        }

        tree_df = df_plot.copy()
        if tree_sort:
            tree_df = sort_items_otros_last(tree_df, label_col="Item", freq_col="Frecuencia", ascending_freq=False)
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
        section_treemap.plotly_chart(fig_tree, use_container_width=True)
        treemap_edited = global_has_edits or any(
            [
                tree_text_mode != "Etiqueta",
                tree_font_size != 14,
                tree_max_depth != 2,
                tree_height != 620,
                tree_pad != 1,
                tree_sort is not True,
                tree_chars_line != 18,
                tree_max_lines != 2,
                tree_auto_font is not True,
                tree_palette_name != "Pastel elegante",
            ]
        )
        add_plotly_exports(fig_tree, "03_treemap", edited=treemap_edited)
        dbg(
            f"treemap rendered height={tree_height} depth={tree_max_depth} palette={tree_palette_name} "
            f"rows={len(tree_df)} edited={treemap_edited}"
        )

    if should_render_graph("Nube de palabras") and not df_plot.empty:
        section_nube = st.expander("Seccion: Nube de palabras", expanded=False)
        with section_nube:
            with st.form(key=f"{key_prefix}_wc_form", border=False):
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
                st.form_submit_button("Aplicar Nube")

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
                section_nube.image(wc_array, caption=f"Nube de palabras - {selected_topic}", width="stretch")
                img_buf = BytesIO()
                Image.fromarray(wc_array).save(img_buf, format="PNG")
                # Cambiar solo el topico visible no cuenta como "edicion" del entregable.
                wc_edited = global_has_edits or any(
                    [
                        max_words != 120,
                        bg != "white",
                        wc_height != 420,
                    ]
                )
                if wc_edited:
                    export_assets["editado/graficos/04_nube_palabras.png"] = img_buf.getvalue()
                remember_original("04_nube_palabras.png", img_buf.getvalue())
                # Guarda parametros para generar TODAS las nubes solo bajo demanda (export).
                st.session_state[f"{key_prefix}_wc_export_params"] = {
                    "max_topicos": len(topic_cols),
                    "max_words": int(max_words),
                    "bg": str(bg),
                    "wc_height": int(wc_height),
                    "file_version": int(file_version),
                    "result_excel": str(result_excel),
                }
                dbg(
                    f"wordcloud rendered topic='{selected_topic}' height={wc_height} max_words={max_words} "
                    f"bg={bg} texts={len(textos)} edited={wc_edited}"
                )

    if should_render_graph("Heatmap"):
        section_heatmap = st.expander("Seccion: Heatmap", expanded=False)
        with section_heatmap:
            with st.form(key=f"{key_prefix}_hm_form", border=False):
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
                st.form_submit_button("Aplicar heatmap")

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
                if not mat.empty:
                    idx_no_otros = [i for i in mat.index if not is_otros_label(i)]
                    idx_otros = [i for i in mat.index if is_otros_label(i)]
                    mat = mat.reindex(idx_no_otros + idx_otros)

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
                    harmonize_plotly_layout(fig_hm, "05_heatmap_posicion")
                    section_heatmap.plotly_chart(fig_hm, use_container_width=True)
                    heatmap_edited = global_has_edits or any(
                        [
                            hm_mode != "Contexto x Posicion",
                            hm_height != 560,
                            hm_color != "Blues",
                        ]
                    )
                    add_plotly_exports(fig_hm, "05_heatmap_posicion", edited=heatmap_edited)
                    dbg(
                        f"heatmap rendered mode=posicion height={hm_height} scale={hm_color} "
                        f"shape={mat.shape} edited={heatmap_edited}"
                    )
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
                    # Para coocurrencia, usa topicos derivados del propio grafo de pares
                    # (evita desalineaciones con etiquetas provenientes de LDC).
                    topic_strength: dict[str, int] = {}
                    for (a, b), w in pairs.items():
                        topic_strength[a] = topic_strength.get(a, 0) + int(w)
                        topic_strength[b] = topic_strength.get(b, 0) + int(w)
                    top_topics = [
                        t for t, _ in sorted(topic_strength.items(), key=lambda kv: kv[1], reverse=True)
                    ][:top_n]
                    mat = pd.DataFrame(0, index=top_topics, columns=top_topics)
                    for (a, b), w in pairs.items():
                        if a in mat.index and b in mat.columns:
                            mat.loc[a, b] += w
                            mat.loc[b, a] += w

                    if int(mat.to_numpy().sum()) == 0:
                        st.info("No hay cruces suficientes dentro del Top N para heatmap de coocurrencia.")
                    else:
                        fig_hm = px.imshow(
                            mat,
                            labels=dict(x="Contexto", y="Contexto", color="Coocurrencia"),
                            color_continuous_scale=hm_color,
                            title="Heatmap: Coocurrencia Contexto x Contexto",
                            aspect="auto",
                        )
                        fig_hm.update_layout(height=hm_height)
                        harmonize_plotly_layout(fig_hm, "05_heatmap_coocurrencia")
                        section_heatmap.plotly_chart(fig_hm, use_container_width=True)
                        heatmap_edited = global_has_edits or any(
                            [
                                hm_mode != "Contexto x Posicion",
                                hm_height != 560,
                                hm_color != "Blues",
                            ]
                        )
                        add_plotly_exports(fig_hm, "05_heatmap_coocurrencia", edited=heatmap_edited)
                        dbg(
                            f"heatmap rendered mode=coocurrencia height={hm_height} scale={hm_color} "
                            f"shape={mat.shape} edited={heatmap_edited}"
                        )

    if should_render_graph("Sankey"):
        section_sankey = st.expander("Seccion: Sankey", expanded=False)
        with section_sankey:
            with st.form(key=f"{key_prefix}_sankey_form", border=False):
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
                st.form_submit_button("Aplicar Sankey")

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
                m1, m2, m3 = section_sankey.columns(3)
                with m1:
                    st.metric("Flujos mostrados", f"{len(rows_links)}")
                with m2:
                    st.metric("Nodos mostrados", f"{len(nodes_ordered)}")
                with m3:
                    st.metric("Flujo mas fuerte", f"n={strongest['value']}")
                section_sankey.caption(f"Flujo principal: {strongest_txt} (n={strongest['value']})")
                section_sankey.plotly_chart(fig_sankey, use_container_width=True)
                sankey_edited = global_has_edits or any(
                    [
                        sankey_mode != "Cadena adyacente completa",
                        sankey_min != 2,
                        sankey_pad != 14,
                        sankey_height != 620,
                        sankey_max_links != 80,
                        sankey_label_len != 28,
                        sankey_thickness != 16,
                        abs(float(sankey_link_alpha) - 0.35) > 1e-9,
                        sankey_font_size != 15,
                        sankey_text_color != "Negro",
                        sankey_font_family != "Verdana",
                        sankey_compact_labels is not True,
                        sankey_hide_self is not True,
                        show_only_top_flows is not True,
                        top_n_flows != 120,
                        abs(float(min_link_pct) - 0.8) > 1e-9,
                        group_similar_labels is not True,
                        abs(float(similarity_threshold) - 0.86) > 1e-9,
                        sankey_arrangement != "snap",
                    ]
                )
                add_plotly_exports(fig_sankey, "06_sankey", edited=sankey_edited)
                dbg(
                    f"sankey rendered mode={sankey_mode} links={len(rows_links)} nodes={len(nodes_ordered)} "
                    f"height={sankey_height} min={sankey_min} max_links={sankey_max_links} edited={sankey_edited}"
                )

    if should_render_graph("Red interactiva"):
        section_red = st.expander("Seccion: Red interactiva", expanded=False)
        with section_red:
            # Controles compactos de reconstruccion (sin duplicar controles JS del panel interno).
            with st.form(key=f"{key_prefix}_net_form", border=False):
                rr1, rr2, rr3, rr4 = st.columns([1.1, 1.1, 1.1, 0.9])
                with rr1:
                    net_height = st.slider("Alto red (px)", min_value=500, max_value=1400, value=900, step=20, key=f"{key_prefix}_net_height")
                with rr2:
                    net_max_nodes = st.slider("Top nodos", min_value=10, max_value=500, value=120, step=10, key=f"{key_prefix}_net_max_nodes")
                with rr3:
                    net_label_len = st.slider("Largo etiqueta", min_value=20, max_value=200, value=120, step=5, key=f"{key_prefix}_net_label_len")
                with rr4:
                    st.markdown("<div style='height:1.85rem'></div>", unsafe_allow_html=True)
                    st.form_submit_button("Aplicar Red", type="primary", use_container_width=True)

            # Parametros que ahora viven en el panel superior HTML interno de la red (sin duplicados en Streamlit).
            net_min_edge = 2
            net_top_edges = 80
            net_font_size = 14
            net_size_scale = 1.2
            net_size_max = 50
        with section_red:
            red_html_path = session_assets_dir / "graficos" / "07_red_interactiva.html"
            max_topicos_red = len([c for c in df_corr.columns if str(c).startswith("Topico_")])
            ok_red, msg_red = build_network_html_from_df(
                df_corr=df_corr,
                out_html_path=red_html_path,
                max_topicos=max_topicos_red,
                min_relacion=int(net_min_edge),
                top_k_edges=int(net_top_edges),
                height_px=700,
                max_nodes=int(net_max_nodes),
                node_font_size=int(net_font_size),
                node_size_scale=float(net_size_scale),
                node_size_max=int(net_size_max),
                label_max_len=int(net_label_len),
            )
            if not ok_red:
                st.warning(msg_red)
            else:
                html_content = red_html_path.read_text(encoding="utf-8", errors="replace")
                html_content = fix_mojibake_text(html_content)
                components.html(html_content, height=net_height, scrolling=True)
                html_bytes = html_content.encode("utf-8")
                red_edited = global_has_edits or any(
                    [
                        net_height != 900,
                        net_max_nodes != 120,
                        net_label_len != 120,
                    ]
                )
                if red_edited:
                    export_assets["editado/graficos/07_red_interactiva.html"] = html_bytes
                remember_original("07_red_interactiva.html", html_bytes)
                save_session_asset("graficos/07_red_interactiva.html", html_bytes)
                dbg(
                    f"red rendered pyvis height={net_height} min_edge={net_min_edge} top_edges={net_top_edges} "
                    f"max_nodes={net_max_nodes} font={net_font_size} scale={net_size_scale} max_size={net_size_max} "
                    f"label_len={net_label_len} edited={red_edited}"
                )

    # Incluye siempre los graficos originales del pipeline (si existen).
    original_assets = {
        k: v for k, v in collect_original_graph_assets(session_assets_dir).items()
        if str(k).startswith("original/graficos/")
    }
    # El snapshot de apertura tiene prioridad para "original al abrir".
    original_assets.update(snapshot_assets)
    export_assets.update(original_assets)
    st.session_state[snapshot_assets_key] = snapshot_assets

    if export_assets:
        st.markdown("**Exportacion**")
        include_all_wc = st.checkbox(
            "Incluir todas las nubes de palabras en exportacion",
            value=True,
            key=f"{key_prefix}_include_all_wc_export",
            help="Genera todas las nubes solo al preparar el ZIP para evitar latencia en cada render.",
        )
        prepare_zip_clicked = st.button("Preparar ZIP de graficos", key=f"{key_prefix}_prepare_zip_btn")
        zip_cache_key = f"{key_prefix}_zip_cache_bytes"
        zip_ready_key = f"{key_prefix}_zip_ready"
        if prepare_zip_clicked:
            export_assets_ready = dict(export_assets)
            if include_all_wc:
                wc_params = st.session_state.get(f"{key_prefix}_wc_export_params", {})
                if wc_params:
                    try:
                        all_wc_assets = build_all_wordcloud_assets(
                            wc_params.get("result_excel", str(result_excel)),
                            int(wc_params.get("file_version", file_version)),
                            max_topicos=int(wc_params.get("max_topicos", 10)),
                            max_words=int(wc_params.get("max_words", 120)),
                            bg=str(wc_params.get("bg", "white")),
                            wc_height=int(wc_params.get("wc_height", 420)),
                        )
                        for rel_name, blob in all_wc_assets.items():
                            export_assets_ready[f"original/graficos/{rel_name}"] = blob
                    except Exception as exc:
                        dbg(f"all_wordclouds_export_error={exc}")
            st.session_state[zip_cache_key] = build_zip_bytes(export_assets_ready)
            st.session_state[zip_ready_key] = True
            dbg(f"zip preparado total_assets={len(export_assets_ready)}")
        st.download_button(
            "Exportar todos (original + editado) (.zip)",
            data=st.session_state.get(zip_cache_key, b""),
            file_name="textana_graficos_original_y_editado.zip",
            mime="application/zip",
            disabled=not bool(st.session_state.get(zip_ready_key, False)),
        )
        dbg(f"export_assets total={len(export_assets)}")

    def render_debug_panel_bottom() -> None:
        if debug_mode:
            st.divider()
            with st.expander("Log diagnostico del visualizador", expanded=True):
                cols_dbg = st.columns([1, 1])
                with cols_dbg[0]:
                    if st.button("Limpiar log debug", key=f"{key_prefix}_clear_debug"):
                        st.session_state[debug_log_key] = []
                        st.rerun()
                with cols_dbg[1]:
                    st.caption(f"Lineas: {len(st.session_state.get(debug_log_key, []))}")
                logs_txt = "\n".join(st.session_state.get(debug_log_key, [])[-200:])
                st.text_area("Debug trace", value=logs_txt, height=260, key=f"{key_prefix}_debug_text", disabled=True)

    if not show_cross_section:
        render_debug_panel_bottom()
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
        render_debug_panel_bottom()
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
        render_debug_panel_bottom()
        return

    if not selected_cross_vars:
        st.info("Selecciona una o mas variables de cruce en la seccion de parametros (parte superior).")
        render_debug_panel_bottom()
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
    prepare_cross_clicked = st.button("Preparar ZIP de cruces", key=f"{key_prefix}_prepare_zip_cross_btn")
    zip_cross_cache_key = f"{key_prefix}_zip_cross_cache_bytes"
    zip_cross_ready_key = f"{key_prefix}_zip_cross_ready"
    if prepare_cross_clicked:
        st.session_state[zip_cross_cache_key] = build_zip_bytes(export_assets)
        st.session_state[zip_cross_ready_key] = True
    st.download_button(
        "Exportar todos + cruces (original + editado) (.zip)",
        data=st.session_state.get(zip_cross_cache_key, b""),
        file_name="textana_graficos_y_cruces_original_y_editado.zip",
        mime="application/zip",
        disabled=not bool(st.session_state.get(zip_cross_ready_key, False)),
    )
    render_debug_panel_bottom()


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
    max_pct_otros: float,
    demo_mode: bool,
    demo_pct: float,
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
        "--max-pct-otros",
        str(max_pct_otros),
        "--output-dir",
        str(OUTPUT_DIR),
    ]

    if demo_mode:
        cmd.extend(["--demo", "--demo-pct", str(demo_pct)])

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
    cleanup_temp_outputs_once(max_age_hours=24)
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
        max_pct_otros = st.number_input("% max 9999 (Otros)", min_value=0.0, max_value=100.0, value=5.0, step=1.0)
        token_file = st.text_input("Archivo token", value="tokenkey.txt")
    with c4:
        demo_mode = st.checkbox("Demo", value=False)
        demo_pct = st.number_input(
            "Demostracion",
            min_value=0.1,
            max_value=100.0,
            value=20.0,
            step=1.0,
            disabled=not demo_mode,
            help="% de casos de la base que se ejecutaran cuando Demo este activo.",
        )
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
            max_pct_otros=float(max_pct_otros),
            demo_mode=bool(demo_mode),
            demo_pct=float(demo_pct),
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
                set_active_assets_for_file(paso3)
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
                up_bytes = bytes(uploaded_result.getbuffer())
                up_sig = _short_hash(f"{uploaded_result.name}|{hashlib.sha1(up_bytes).hexdigest()}", size=24)
                if st.session_state.get("last_result_upload_sig") != up_sig:
                    loaded_target = save_uploaded_result_excel(
                        up_bytes,
                        stem="Paso3_clasificacion_cargado",
                        source_name=uploaded_result.name,
                    )
                    set_active_assets_for_file(loaded_target)
                    st.session_state["result_excel_path"] = str(loaded_target)
                    st.session_state["allow_graphs_from_upload"] = True
                    st.session_state["last_result_upload_sig"] = up_sig
                    st.success(f"Archivo cargado: {uploaded_result.name}")
                else:
                    st.caption(f"Archivo ya cargado en esta sesion: {uploaded_result.name}")
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
            secure_raw = uploaded_secure.getvalue()
            secure_sig = _short_hash(f"{uploaded_secure.name}|{hashlib.sha1(secure_raw).hexdigest()}", size=24)
            if st.session_state.get("last_secure_upload_sig") == secure_sig:
                st.caption(f"Paquete ya cargado en esta sesion: {uploaded_secure.name}")
                ok, msg, payload_bytes, src_name, extra_files = False, "", None, None, {}
            else:
                ok, msg, payload_bytes, src_name, extra_files = validate_secure_textana_package(secure_raw)
            if ok and payload_bytes is not None:
                try:
                    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(src_name or "resultado").stem).strip("._") or "resultado"
                    loaded_target = save_uploaded_result_excel(
                        payload_bytes,
                        stem=f"{safe_stem}_secure_cargado",
                        source_name=src_name or uploaded_secure.name,
                    )
                    assets_dir = set_active_assets_for_file(loaded_target)
                    restore_viewer_extra_files(extra_files, assets_dir=assets_dir)
                    st.session_state["result_excel_path"] = str(loaded_target)
                    st.session_state["allow_graphs_from_upload"] = True
                    st.session_state["last_secure_upload_sig"] = secure_sig
                    st.success(f"{msg} Archivo cargado: {Path(src_name or 'resultado.xlsx').name}")
                except PermissionError:
                    st.error("No se pudo guardar el paquete cargado. Cierra Excel si hay archivos de output abiertos e intenta de nuevo.")
                except Exception as exc:
                    st.error(f"No se pudo guardar el paquete cargado: {exc}")
            elif msg:
                st.error(msg)

    result_excel = Path(st.session_state.get("result_excel_path", ""))
    result_token = ""
    if result_excel and result_excel.exists():
        try:
            stt = result_excel.stat()
            result_token = f"{result_excel.resolve()}|{stt.st_mtime_ns}|{stt.st_size}"
        except Exception:
            result_token = str(result_excel)
    if st.session_state.get("main_result_token") != result_token:
        st.session_state["main_result_token"] = result_token
        st.session_state.pop("main_secure_pkg_bytes", None)
        st.session_state.pop("main_secure_pkg_ready", None)
    graphs_enabled = bool(
        (st.session_state.get("allow_graphs_from_run", False) or st.session_state.get("allow_graphs_from_upload", False))
        and result_excel
        and result_excel.exists()
    )

    current_assets_dir = None
    if graphs_enabled:
        current_assets_dir = set_active_assets_for_file(result_excel)

    with down_col:
        if graphs_enabled:
            excel_complete, excel_issues = check_viewer_excel_completeness(result_excel)
            fecha_tag = datetime.now().strftime("%Y%m%d")
            hoja_tag_raw = clean_text_value(sheet) or "Hoja"
            col_tag_raw = clean_text_value(cols_sel[0] if cols_sel else "") or "Columna"

            def _safe_name(value: str) -> str:
                txt = unicodedata.normalize("NFKD", str(value))
                txt = txt.encode("ascii", "ignore").decode("ascii")
                txt = re.sub(r"[^A-Za-z0-9._-]+", "_", txt).strip("._")
                return txt or "NA"

            export_name = f"{fecha_tag}_{_safe_name(hoja_tag_raw)}_{_safe_name(col_tag_raw)}_Report.xlsx"
            st.download_button(
                "Exportar Reporte (.xlsx)",
                data=result_excel.read_bytes(),
                file_name=export_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            if not excel_complete:
                st.warning("El archivo actual no contiene toda la estructura esperada para mostrar todos los graficos en el viewer:")
                for it in excel_issues:
                    st.caption(f"- {it}")
            if secure_package_available() and excel_complete and current_assets_dir is not None:
                pkg_cache_key = "main_secure_pkg_bytes"
                pkg_ready_key = "main_secure_pkg_ready"
                prepare_pkg = st.button("Preparar paquete seguro (.textana)", use_container_width=True)
                if prepare_pkg:
                    secure_pkg_bytes = build_secure_textana_package(
                        result_excel.read_bytes(),
                        source_name=result_excel.name,
                        extra_files=collect_viewer_extra_files(current_assets_dir),
                    )
                    st.session_state[pkg_cache_key] = secure_pkg_bytes
                    st.session_state[pkg_ready_key] = True
                st.download_button(
                    "Exportar paquete seguro (.textana)",
                    data=st.session_state.get(pkg_cache_key, b""),
                    file_name=f"{result_excel.stem}.textana",
                    mime="application/octet-stream",
                    use_container_width=True,
                    disabled=not bool(st.session_state.get(pkg_ready_key, False)),
                )
            else:
                st.button("Exportar paquete seguro (.textana)", disabled=True, use_container_width=True)
        else:
            st.button("Exportar Reporte (.xlsx)", disabled=True, use_container_width=True)
            st.button("Exportar paquete seguro (.textana)", disabled=True, use_container_width=True)

    if graphs_enabled and current_assets_dir is not None:
        render_visual_dashboard(result_excel, selected_cross_vars_param=cross_vars_param, assets_dir=current_assets_dir)
    else:
        st.info("Los graficos se habilitan solo cuando: 1) ejecutas Textana completo con exito, o 2) cargas un Excel de resultados.")


if __name__ == "__main__":
    main()
