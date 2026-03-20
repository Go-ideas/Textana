#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from streamlit_app import (
    OUTPUT_DIR,
    apply_layout_tweaks,
    build_secure_textana_package,
    check_viewer_excel_completeness,
    clean_text_value,
    collect_viewer_extra_files,
    is_otros_label,
    render_visual_dashboard,
    restore_viewer_extra_files,
    secure_package_available,
    sort_items_otros_last,
    validate_secure_textana_package,
)


def save_secure_payload(payload_bytes: bytes, source_name: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(source_name or "resultado").stem).strip("._") or "resultado"
    target = OUTPUT_DIR / f"{stem}_viewer_{time.time_ns()}.xlsx"
    target.write_bytes(payload_bytes)
    return target


def load_excel_sheets(path: Path) -> dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    return {sn: pd.read_excel(path, sheet_name=sn, engine="openpyxl") for sn in xls.sheet_names}


def sorted_topic_cols(df_corr: pd.DataFrame) -> list[str]:
    cols = [c for c in df_corr.columns if str(c).startswith("Topico_")]
    return sorted(cols, key=lambda x: int(str(x).split("_")[-1]) if str(x).split("_")[-1].isdigit() else str(x))


def rebuild_ldc_from_corr(df_corr: pd.DataFrame, prev_ldc: pd.DataFrame | None = None) -> pd.DataFrame:
    topic_cols = sorted_topic_cols(df_corr)
    counter: dict[str, int] = {}
    for c in topic_cols:
        vals = df_corr[c].apply(clean_text_value)
        for v in vals:
            if not v:
                continue
            counter[v] = counter.get(v, 0) + 1

    items = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
    total = sum(v for _, v in items) or 1

    map_topico: dict[str, str] = {}
    map_cod: dict[str, object] = {}
    if prev_ldc is not None and not prev_ldc.empty:
        has_ctx = "Contexto" in prev_ldc.columns
        has_top = "Topico" in prev_ldc.columns
        for _, r in prev_ldc.iterrows():
            key = clean_text_value(r.get("Contexto" if has_ctx else "Topico", ""))
            if not key:
                continue
            if has_top:
                map_topico[key] = clean_text_value(r.get("Topico", "")) or key
            if "COD_Topico" in prev_ldc.columns:
                map_cod[key] = r.get("COD_Topico")

    rows = []
    for ctx, freq in items:
        cod_value = map_cod.get(ctx, None)
        if is_otros_label(ctx):
            cod_value = 9999
        row = {
            "Contexto": ctx,
            "Topico": map_topico.get(ctx, ctx),
            "COD_Topico": cod_value,
            "Frecuencia": int(freq),
            "Porcentaje": float(freq) / float(total) * 100.0,
        }
        rows.append(row)
    out = pd.DataFrame(rows)
    if not out.empty and "Contexto" in out.columns:
        out = sort_items_otros_last(out, label_col="Contexto", freq_col="Frecuencia", ascending_freq=False)

    # Respeta estructura previa si existe para no romper entregables.
    if prev_ldc is not None and not prev_ldc.empty:
        cols = [str(c) for c in prev_ldc.columns]
        for c in cols:
            if c not in out.columns:
                out[c] = None
        out = out[cols]
    return out


def persist_excel_updates(excel_path: Path, df_corr_new: pd.DataFrame) -> None:
    sheets = load_excel_sheets(excel_path)
    prev_ldc = sheets.get("LDC")
    ldc_new = rebuild_ldc_from_corr(df_corr_new, prev_ldc=prev_ldc)

    sheets["clasificacion_sin_incorrectos"] = df_corr_new
    if "clasificacion_correctos" in sheets:
        sheets["clasificacion_correctos"] = df_corr_new.copy()
    if "clasificacion" in sheets:
        base = sheets["clasificacion"]
        if len(base) == len(df_corr_new):
            merged = base.copy()
            for c in df_corr_new.columns:
                if c in merged.columns:
                    merged[c] = df_corr_new[c].values
            sheets["clasificacion"] = merged
    sheets["LDC"] = ldc_new

    order = list(sheets.keys())
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as wr:
        for sn in order:
            sheets[sn].to_excel(wr, sheet_name=sn, index=False)


def _apply_label_map(value, mapping: dict[str, str]):
    key = clean_text_value(value)
    if not key:
        return value
    return mapping.get(key, value)


def apply_global_redaction_updates(
    excel_path: Path,
    context_map: dict[str, str],
    topic_map: dict[str, str],
) -> None:
    sheets = load_excel_sheets(excel_path)
    if "clasificacion_sin_incorrectos" not in sheets:
        raise ValueError("Falta hoja `clasificacion_sin_incorrectos` para aplicar redaccion global.")

    df_corr = sheets["clasificacion_sin_incorrectos"].copy()
    topic_cols = sorted_topic_cols(df_corr)
    for c in topic_cols:
        df_corr[c] = df_corr[c].apply(lambda v: _apply_label_map(v, context_map))

    # Propaga cambios de contexto/topico en hojas de clasificacion si existen.
    for sn in ["clasificacion_sin_incorrectos", "clasificacion_correctos", "clasificacion"]:
        if sn not in sheets:
            continue
        dfx = sheets[sn].copy()
        cols_topic = [c for c in dfx.columns if str(c).startswith("Topico_")]
        for c in cols_topic:
            dfx[c] = dfx[c].apply(lambda v: _apply_label_map(v, context_map))
        if "Topico" in dfx.columns:
            dfx["Topico"] = dfx["Topico"].apply(lambda v: _apply_label_map(v, topic_map))
        sheets[sn] = dfx

    prev_ldc = sheets.get("LDC")
    if prev_ldc is not None and not prev_ldc.empty:
        ldc_tmp = prev_ldc.copy()
        if "Contexto" in ldc_tmp.columns:
            ldc_tmp["Contexto"] = ldc_tmp["Contexto"].apply(lambda v: _apply_label_map(v, context_map))
        if "Topico" in ldc_tmp.columns:
            ldc_tmp["Topico"] = ldc_tmp["Topico"].apply(lambda v: _apply_label_map(v, topic_map))
        prev_ldc = ldc_tmp

    ldc_new = rebuild_ldc_from_corr(sheets["clasificacion_sin_incorrectos"], prev_ldc=prev_ldc)
    if "Topico" in ldc_new.columns:
        ldc_new["Topico"] = ldc_new["Topico"].apply(lambda v: _apply_label_map(v, topic_map))
    sheets["LDC"] = ldc_new

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as wr:
        for sn, df in sheets.items():
            df.to_excel(wr, sheet_name=sn, index=False)


def render_editor(excel_path: Path) -> bool:
    st.divider()
    st.subheader("Editor global de redaccion (contextos y topicos)")
    try:
        sheets = load_excel_sheets(excel_path)
        df_corr = sheets["clasificacion_sin_incorrectos"].copy()
    except Exception as exc:
        st.error(f"No se pudo abrir el entregable para edicion global: {exc}")
        return False

    topic_cols = sorted_topic_cols(df_corr)
    if not topic_cols:
        st.info("No hay columnas `Topico_*` para redaccion global.")
        return False

    # Construye catalogo unico de contextos para redaccion global.
    vals = []
    for c in topic_cols:
        vals.extend(df_corr[c].apply(clean_text_value).tolist())
    uniq_contexts = sorted({v for v in vals if v})
    freq_map = {k: 0 for k in uniq_contexts}
    for c in topic_cols:
        for v in df_corr[c].apply(clean_text_value):
            if v in freq_map:
                freq_map[v] += 1

    ldc = sheets.get("LDC", pd.DataFrame())
    topico_map_by_context: dict[str, str] = {}
    cod_map_by_context: dict[str, object] = {}
    if not ldc.empty and "Contexto" in ldc.columns and "Topico" in ldc.columns:
        for _, r in ldc.iterrows():
            ctx = clean_text_value(r.get("Contexto", ""))
            top = clean_text_value(r.get("Topico", ""))
            if ctx:
                topico_map_by_context[ctx] = top or ctx
                if "COD_Topico" in ldc.columns:
                    cod_map_by_context[ctx] = r.get("COD_Topico")

    rows = []
    for ctx in uniq_contexts:
        top = topico_map_by_context.get(ctx, ctx)
        rows.append(
            {
                "COD_Actual": cod_map_by_context.get(ctx, 9999 if is_otros_label(ctx) else None),
                "Topico_Actual": top,
                "Contexto_Actual": ctx,
                "Frecuencia": int(freq_map.get(ctx, 0)),
                "Nuevo_COD": cod_map_by_context.get(ctx, 9999 if is_otros_label(ctx) else None),
                "Nuevo_Topico": top,
                "Nuevo_Contexto": ctx,
            }
        )
    edit_df = pd.DataFrame(rows)
    # Orden numerico real por codigo (menor->mayor), no alfabetico.
    edit_df["COD_Orden"] = pd.to_numeric(edit_df["COD_Actual"], errors="coerce")
    edit_df = (
        edit_df.sort_values(
            ["COD_Orden", "Topico_Actual", "Contexto_Actual"],
            ascending=[True, True, True],
            na_position="last",
        )
        .drop(columns=["COD_Orden"])
        .reset_index(drop=True)
    )
    edit_df = edit_df[
        [
            "COD_Actual",
            "Topico_Actual",
            "Contexto_Actual",
            "Nuevo_COD",
            "Nuevo_Topico",
            "Nuevo_Contexto",
            "Frecuencia",
        ]
    ]

    edited = st.data_editor(
        edit_df,
        use_container_width=True,
        hide_index=True,
        key="viewer_global_redaction_grid",
        column_config={
            "COD_Actual": st.column_config.NumberColumn("COD actual", disabled=True, format="%d"),
            "Contexto_Actual": st.column_config.TextColumn("Contexto actual", disabled=True),
            "Topico_Actual": st.column_config.TextColumn("Topico actual", disabled=True),
            "Nuevo_COD": st.column_config.NumberColumn("Nuevo COD", format="%d"),
            "Frecuencia": st.column_config.NumberColumn("Frecuencia", disabled=True),
            "Nuevo_Contexto": st.column_config.TextColumn("Nueva redaccion contexto"),
            "Nuevo_Topico": st.column_config.TextColumn("Nueva redaccion topico"),
        },
    )
    if st.button("Aplicar redaccion global y actualizar entregable + graficos", type="primary", use_container_width=True):
        try:
            context_map: dict[str, str] = {}
            topic_map: dict[str, str] = {}
            cod_map: dict[str, object] = {}
            for _, r in edited.iterrows():
                old_ctx = clean_text_value(r.get("Contexto_Actual", ""))
                new_ctx = clean_text_value(r.get("Nuevo_Contexto", ""))
                old_top = clean_text_value(r.get("Topico_Actual", ""))
                new_top = clean_text_value(r.get("Nuevo_Topico", ""))
                old_cod = r.get("COD_Actual")
                new_cod = r.get("Nuevo_COD")
                if old_ctx and new_ctx and old_ctx != new_ctx:
                    context_map[old_ctx] = new_ctx
                if old_top and new_top and old_top != new_top:
                    topic_map[old_top] = new_top
                if old_ctx:
                    if is_otros_label(new_ctx or old_ctx):
                        cod_map[old_ctx] = 9999
                    elif str(new_cod).strip() not in {"", "nan", "None", "null"} and str(new_cod) != str(old_cod):
                        cod_map[old_ctx] = new_cod

            if not context_map and not topic_map and not cod_map:
                st.info("No hay cambios globales para aplicar.")
                return False

            apply_global_redaction_updates(excel_path, context_map=context_map, topic_map=topic_map)
            if cod_map:
                sheets_after = load_excel_sheets(excel_path)
                if "LDC" in sheets_after:
                    ldc_after = sheets_after["LDC"].copy()
                    if "Contexto" in ldc_after.columns and "COD_Topico" in ldc_after.columns:
                        for old_ctx, new_cod in cod_map.items():
                            mask = ldc_after["Contexto"].apply(clean_text_value) == clean_text_value(context_map.get(old_ctx, old_ctx))
                            ldc_after.loc[mask, "COD_Topico"] = new_cod
                        with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as wr:
                            for sn, df in sheets_after.items():
                                if sn == "LDC":
                                    ldc_after.to_excel(wr, sheet_name=sn, index=False)
                                else:
                                    df.to_excel(wr, sheet_name=sn, index=False)
            st.success("Redaccion global aplicada. El entregable y los graficos se actualizaron.")
            return True
        except Exception as exc:
            st.error(f"No se pudo aplicar la redaccion global: {exc}")
    return False


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
    if "viewer_has_valid_payload" not in st.session_state:
        st.session_state["viewer_has_valid_payload"] = False
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
                st.session_state["viewer_has_valid_payload"] = True
                st.success(f"{msg} Archivo: {Path(src_name or 'resultado.xlsx').name}")
            except Exception as exc:
                st.session_state["viewer_has_valid_payload"] = False
                st.error(f"No se pudo preparar el archivo para visualizacion: {exc}")
        else:
            st.session_state["viewer_has_valid_payload"] = False
            st.error(msg)

    excel_raw = str(st.session_state.get("viewer_result_excel_path", "")).strip()
    excel_path = Path(excel_raw) if excel_raw else None
    can_render = bool(
        st.session_state.get("viewer_has_valid_payload", False)
        and excel_path is not None
        and excel_path.exists()
        and excel_path.is_file()
    )
    if can_render and excel_path is not None:
        ok_full, issues = check_viewer_excel_completeness(excel_path)
        if not ok_full:
            st.warning("El paquete cargado no trae toda la estructura para mostrar todos los graficos:")
            for it in issues:
                st.caption(f"- {it}")
        d1, d2 = st.columns(2)
        with d1:
            st.download_button(
                "Descargar paquete .textana cargado",
                data=st.session_state["viewer_pkg_bytes"],
                file_name=st.session_state["viewer_pkg_name"],
                mime="application/octet-stream",
                disabled=(len(st.session_state["viewer_pkg_bytes"]) == 0),
                use_container_width=True,
            )
        with d2:
            st.download_button(
                "Descargar Excel final (editado)",
                data=excel_path.read_bytes(),
                file_name=f"{excel_path.stem}_final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        if render_editor(excel_path):
            st.rerun()

        if secure_package_available():
            try:
                pkg_updated = build_secure_textana_package(
                    excel_path.read_bytes(),
                    source_name=excel_path.name,
                    extra_files=collect_viewer_extra_files(),
                )
                st.download_button(
                    "Descargar paquete .textana actualizado",
                    data=pkg_updated,
                    file_name=f"{excel_path.stem}_actualizado.textana",
                    mime="application/octet-stream",
                    use_container_width=True,
                )
            except Exception as exc:
                st.warning(f"No se pudo generar paquete actualizado: {exc}")
        st.divider()
        st.header("Graficos en la plataforma")
        render_visual_dashboard(excel_path, selected_cross_vars_param=[], show_cross_section=False)
    else:
        st.info("Carga un archivo .textana valido para habilitar el visualizador.")


if __name__ == "__main__":
    main()
