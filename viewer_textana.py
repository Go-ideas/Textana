#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import time
from pathlib import Path

import streamlit as st

from streamlit_app import (
    OUTPUT_DIR,
    apply_layout_tweaks,
    check_viewer_excel_completeness,
    render_visual_dashboard,
    restore_viewer_extra_files,
    validate_secure_textana_package,
)


def save_secure_payload(payload_bytes: bytes, source_name: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(source_name or "resultado").stem).strip("._") or "resultado"
    target = OUTPUT_DIR / f"{stem}_viewer_{time.time_ns()}.xlsx"
    target.write_bytes(payload_bytes)
    return target


def main() -> None:
    st.set_page_config(page_title="Textana Viewer", layout="wide")
    apply_layout_tweaks()
    logo_path = Path(__file__).resolve().parent / "Propuesta 4 32.png"
    if logo_path.exists():
        st.image(str(logo_path), width=220)
    st.title("Textana Viewer - Paquetes .textana")
    st.write("Carga un paquete .textana para visualizar y editar graficos. No se expone la tabla cruda al cliente.")

    if "viewer_result_excel_path" not in st.session_state:
        st.session_state["viewer_result_excel_path"] = ""
    if "viewer_pkg_bytes" not in st.session_state:
        st.session_state["viewer_pkg_bytes"] = b""
    if "viewer_pkg_name" not in st.session_state:
        st.session_state["viewer_pkg_name"] = "paquete.textana"

    uploaded = st.file_uploader("Cargar paquete seguro (.textana)", type=["textana"], key="viewer_pkg_upload")
    if uploaded is not None:
        ok, msg, payload_bytes, src_name, extra_files = validate_secure_textana_package(uploaded.getvalue())
        if ok and payload_bytes is not None:
            try:
                path = save_secure_payload(payload_bytes, src_name or "resultado.xlsx")
                restore_viewer_extra_files(extra_files)
                st.session_state["viewer_result_excel_path"] = str(path)
                st.session_state["viewer_pkg_bytes"] = uploaded.getvalue()
                st.session_state["viewer_pkg_name"] = uploaded.name or "paquete.textana"
                st.success(f"{msg} Archivo: {Path(src_name or 'resultado.xlsx').name}")
            except Exception as exc:
                st.error(f"No se pudo preparar el archivo para visualizacion: {exc}")
        else:
            st.error(msg)

    excel_path = Path(st.session_state.get("viewer_result_excel_path", ""))
    if excel_path and excel_path.exists():
        ok_full, issues = check_viewer_excel_completeness(excel_path)
        if not ok_full:
            st.warning("El paquete cargado no trae toda la estructura para mostrar todos los graficos:")
            for it in issues:
                st.caption(f"- {it}")
        st.download_button(
            "Descargar paquete .textana cargado",
            data=st.session_state["viewer_pkg_bytes"],
            file_name=st.session_state["viewer_pkg_name"],
            mime="application/octet-stream",
            disabled=(len(st.session_state["viewer_pkg_bytes"]) == 0),
        )
        st.divider()
        st.header("Graficos en la plataforma")
        render_visual_dashboard(excel_path, selected_cross_vars_param=[], show_cross_section=False)
    else:
        st.info("Carga un archivo .textana valido para habilitar el visualizador.")


if __name__ == "__main__":
    main()
