# Textana Viewer (.textana)

## Ejecutar local
```powershell
python -m streamlit run viewer_textana.py
```

## Publicar gratis (Streamlit Community Cloud)
1. Sube este repo a GitHub.
2. Crea una app en Streamlit Cloud apuntando a `viewer_textana.py`.
3. En *Secrets* configura:
   - `TEXTANA_SIGN_KEY = "<tu_clave_compartida>"`

## Notas
- El viewer solo acepta paquetes `.textana`.
- Si la clave no coincide con la usada para exportar, el paquete no abre.
- El cliente edita graficos en la app, sin exponer Excel crudo.
