# Guía de Contribución al proyecto

Este documento define las normas de colaboración en este repositorio.

> **Importante:** Tanto **los nombres de las ramas** como **todos los mensajes de commit** deben estar **en inglés**.  
> (Las descripciones de Issues/PR pueden estar en español).

---

## 1) Modelo de ramas

- `main`: rama estable; solo código listo para entrega/demos.
- `dev`: rama de integración de sprint; se mergean aquí las features antes de pasar a `main`.
- Ramas de trabajo:
  - `feature/<short-name>` → nuevas funcionalidades  
    Ej.: `feature/data-ingestion`, `feature/powerbi-dashboard`
  - `fix/<short-name>` → correcciones  
    Ej.: `fix/fix-null-values`
  - `docs/<short-name>` → documentación  
    Ej.: `docs/update-readme`
  - `hotfix/<short-name>` → arreglos urgentes sobre `main` (excepcional)

**Reglas**

- Crear ramas **desde `dev`** (salvo `hotfix/`).
- Nombre de rama **en inglés**, corto y descriptivo.
- No trabajar directamente sobre `main`.

---

## 2) Commits

**Formato recomendado (en inglés, modo imperativo):**
`type: short description in English`

**Tipos:** `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`

**Ejemplos**

- `feat: add demand ingestion script`
- `fix: correct unit normalization on price data`
- `docs: update setup instructions for Fabric`

**Buenas prácticas**

- Título ≤ 72 caracteres.
- Mensaje claro; añade cuerpo si hace falta contexto.

---

## 3) Pull Requests

- Cada PR debe:
  - Estar **vinculado a una issue** (p. ej., `Closes #12`).
  - Usar la plantilla `.github/pull_request_template.md`.
  - Pasar validaciones locales (tests/lint si aplica).
  - Tener **≥ 1 aprobación** antes de merge.

**Política de merge**

- Usar **“Squash and merge”** (historial limpio).
- Título del squash commit **en inglés** y descriptivo.

---

## 4) Issues

- Toda tarea debe tener una **issue** con criterios de aceptación.
- Asignar **Milestone** (Sprint 1/2/3) y **labels** (`Bronze`, `Silver`, `Gold`, `Dashboard`, `Docs`, etc.).
- Descripción en español aceptada.

---

## 5) Daily Standups

Registro en una issue fija: **“Daily Standup Log”**.

Cada persona comenta a diario con:

[AAAA-MM-DD] Nombre
Ayer: ...
Hoy: ...
Bloqueos: ...

---

## 6) Buenas prácticas

- Documentar scripts/notebooks/dashboards (README y /docs).
- No subir datos sensibles ni ficheros grandes (usar `.gitignore`).
- Mantener el Project actualizado (estado, milestone y etiquetas).
