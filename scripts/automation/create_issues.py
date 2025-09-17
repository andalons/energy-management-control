import os
from datetime import datetime, timedelta
from github import Github, GithubException, Auth

# ========= CONFIG =========
REPO_FULL_NAME = "andalons/energy-management-control"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Fechas objetivo (ajusta a tu calendario real, aquí aproximamos 3 sprints/semana)
today = datetime.today()
MILESTONES_CFG = [
    {"title": "Sprint 1 — Bronze", "description": "Setup + Ingesta (Bronze)", "due_on": (today + timedelta(days=10))},
    {"title": "Sprint 2 — Silver + EDA", "description": "Limpieza/Enriquecimiento (Silver) + EDA", "due_on": (today + timedelta(days=17))},
    {"title": "Sprint 3 — Gold + Dashboards + Docs + Optional", "description": "Gold + Power BI + Documentación + Multiagente", "due_on": (today + timedelta(days=24))},
]

LABELS = [
    ("Bronze", "B28600"),
    ("Silver", "A8A8A8"),
    ("Gold", "DAA520"),
    ("Dashboard", "0E8A16"),
    ("Analysis", "1D76DB"),
    ("Docs", "5319E7"),
    ("Optional", "8B949E"),
    ("Blocked", "D73A4A"),
    ("Help Wanted", "008672"),
]

# ========= HELPERS =========
DOD_DEFAULT = [
    "Checklist completado al 100%.",
    "Código versionado en el repo con README/uso básico.",
    "Logs/validaciones (si aplica) sin errores.",
    "Documentación actualizada (CHANGELOG o nota en README).",
]

def md_checklist(items):
    return "\n".join([f"- [ ] {it}" for it in items])

def md_list(items):
    return "\n".join([f"- {it}" for it in items])

def build_body(sprint, context, checklist, refs=None, dod=None):
    refs_md = ""
    if refs:
        refs_md = "\n**Referencias / Notas:**\n" + md_list(refs) + "\n"
    dod_md = md_list(dod or DOD_DEFAULT)
    return (
        f"**Sprint:** {sprint}\n\n"
        f"**Contexto:** {context}\n\n"
        f"**Checklist detallado:**\n{md_checklist(checklist)}\n"
        f"{refs_md}\n"
        f"**Definition of Done (DoD):**\n{dod_md}\n"
    )

# ========= BACKLOG DETALLADO =========
SPRINT1_ISSUES = [
    dict(
        title="Crear Workspace en Fabric y convenciones de nombres",
        labels=["Docs"],
        context="Crear y documentar el Workspace base en Fabric y las convenciones (nombres de Lakehouse, notebooks, pipelines y PBIX).",
        checklist=[
            "Crear Workspace en Fabric.",
            "Definir nomenclatura: lakehouse `lh-energy-{env}`, notebooks `nb-...`, pipelines `pl-...`, PBIX `dash-...`.",
            "Crear estructura inicial en Fabric: carpetas para Bronze/Silver/Gold.",
            "Documentar en README: acceso, roles (lectura/escritura), y límites.",
        ],
        refs=["Fabric Workspaces (docs internas del equipo, si aplica)"],
    ),
    dict(
        title="Estructura del repo + plantillas README/CONTRIBUTING",
        labels=["Docs"],
        context="Estructurar repo con carpetas `/scripts`, `/notebooks`, `/docs`, `/dashboards`, `/infra` y plantillas.",
        checklist=[
            "Inicializar repo con README y LICENSE.",
            "Crear `/scripts`, `/notebooks`, `/docs`, `/dashboards`, `/infra`.",
            "Añadir CONTRIBUTING.md con estilo de commits y PRs.",
            "Añadir `.gitignore` (Python, notebooks, PBIX).",
            "Subir plantilla de README por módulo.",
        ],
        refs=["Convenciones del equipo", "Plantillas internas si existen"],
    ),
    dict(
        title="Definir estándares Lakehouse (carpetas Bronze/Silver/Gold)",
        labels=["Bronze"],
        context="Definir la estructura lógica y física del lakehouse para seguir el patrón Medallion.",
        checklist=[
            "Decidir rutas/paths para `/bronze`, `/silver`, `/gold`.",
            "Definir formatos (Delta/Parquet) y particionamiento (por fecha).",
            "Definir política de retención y versionado (si aplica).",
            "Documentar en `/docs/medallion.md`.",
        ],
        refs=["Arquitectura Medallion (resumen del proyecto)"],
    ),
    dict(
        title="Explorar API REData (endpoints, límites, 24m)",
        labels=["Bronze"],
        context="Revisión funcional de endpoints de REData, janelas de consulta y límites.",
        checklist=[
            "Listar endpoints clave: demanda, generación por tecnología, intercambios, precios.",
            "Anotar límites (intervalos, paginación si aplica, time_trunc).",
            "Probar requests con `requests` / `httpx`.",
            "Guardar ejemplos JSON en `/docs/samples/`.",
        ],
        refs=["Specs de REData (endpoints y límites conocidos)"],
    ),
    dict(
        title="Módulo reddata_client.py + tests mínimos",
        labels=["Bronze","Analysis"],
        context="Cliente Python reutilizable para consultar REData con manejo de errores, reintentos y logging.",
        checklist=[
            "Implementar `reddata_client.py` con funciones por endpoint.",
            "Añadir manejo de errores HTTP y reintentos exponenciales.",
            "Parametrizar fechas y `time_trunc`.",
            "Tests mínimos con `pytest` (mock responses).",
            "Ejemplos de uso en README.",
        ],
        refs=["`requests`/`httpx` mejores prácticas", "pytest básico"],
    ),
    dict(
        title="Ingesta demanda horaria → Bronze (Delta/Parquet)",
        labels=["Bronze"],
        context="Extraer demanda horaria histórica y guardarla en Bronze con esquema consistente.",
        checklist=[
            "Script/Notebook para demanda horaria.",
            "Normalizar campos base (timestamp, valor, ámbito).",
            "Guardar en Delta/Parquet en `/bronze/demanda/`.",
            "Validar conteos y nulls.",
            "Log de ejecución y tiempos.",
        ],
        refs=["Decisiones de esquema en `/docs/medallion.md`"],
    ),
    dict(
        title="Ingesta generación por tecnología → Bronze",
        labels=["Bronze"],
        context="Extraer generación desglosada por tecnología a Bronze.",
        checklist=[
            "Script/Notebook para generación por tecnología.",
            "Mapeo preliminar de tecnologías (sin aún agrupar renovables).",
            "Guardar en `/bronze/generacion_tecnologia/`.",
            "Validar duplicados por timestamp/tech.",
        ],
        refs=[],
    ),
    dict(
        title="Ingesta intercambios internacionales → Bronze",
        labels=["Bronze"],
        context="Extraer intercambios (import/export) por frontera a Bronze.",
        checklist=[
            "Script/Notebook para intercambios.",
            "Normalizar signo (importaciones positivas, export negativas o viceversa, decidir).",
            "Guardar en `/bronze/intercambios/`.",
            "Validar rangos y coherencia temporal.",
        ],
        refs=[],
    ),
    dict(
        title="Ingesta precios mercado → Bronze",
        labels=["Bronze"],
        context="Extraer precio marginal horario (OMIE) a Bronze.",
        checklist=[
            "Script/Notebook para precios.",
            "Normalizar moneda y unidad (€/MWh).",
            "Guardar en `/bronze/precios/`.",
            "Cruce rápido con demanda/generación para sanity check.",
        ],
        refs=[],
    ),
    dict(
        title="Pipeline incremental (programación + logs)",
        labels=["Bronze"],
        context="Orquestar la ingesta incremental (diaria/horaria) con logging y notificaciones.",
        checklist=[
            "Definir frecuencia (ej. diario) y ventana de actualización.",
            "Diseñar orquestación (Fabric/Pipeline).",
            "Escribir logs a storage o table técnica.",
            "Manejo de errores + alerta (email/teams si aplica).",
        ],
        refs=[],
    ),
    dict(
        title="Validaciones Bronze: conteos, esquema, duplicados",
        labels=["Bronze"],
        context="Añadir data checks básicos sobre tablas Bronze.",
        checklist=[
            "Conteo por día/endpoint.",
            "Esquema: tipos correctos (timestamp, numéricos).",
            "Duplicados por keys naturales.",
            "Reporte semanal de calidad (markdown).",
        ],
        refs=[],
    ),
    dict(
        title="Exportar notebooks Fabric al repo",
        labels=["Docs"],
        context="Exportar notebooks (ipynb/py) a GitHub para versionado.",
        checklist=[
            "Estándar de nombres `nb-XXXXX.ipynb`.",
            "Exportar a `/notebooks/bronze/`.",
            "Añadir instrucciones de import/export en README.",
        ],
        refs=[],
    ),
    dict(
        title="Documentar ejecución y troubleshooting",
        labels=["Docs"],
        context="Guía práctica para ejecutar ingestas, errores comunes y soluciones.",
        checklist=[
            "Crear `/docs/runbook_ingesta.md`.",
            "Añadir secciones de errores REData (500, 429, timeouts).",
            "Cómo relanzar una tarea fallida.",
        ],
        refs=[],
    ),
    dict(
        title="Revisión Sprint 1 + demo corta",
        labels=["Docs"],
        context="Revisión de alcance Sprint 1 y demo funcional de ingesta Bronze.",
        checklist=[
            "Checklist de issues cerrados.",
            "Demostración lectura de tablas Bronze.",
            "Minutas de la review y acuerdos.",
        ],
        refs=[],
    ),
]

SPRINT2_ISSUES = [
    dict(
        title="Normalizar timestamps y zonas horarias (CET/CEST)",
        labels=["Silver"],
        context="Uniformar timestamps a UTC o CET y documentar conversión estacional.",
        checklist=[
            "Decidir TZ canónica (recomendado UTC).",
            "Añadir columna original y columna normalizada.",
            "Documentar tratamiento CEST/CET.",
        ],
        refs=[],
    ),
    dict(
        title="Limpieza: nulos, duplicados, negativos espurios",
        labels=["Silver"],
        context="Aplicar reglas de limpieza sobre Bronze para producir Silver limpio.",
        checklist=[
            "Especificar reglas (drop/impute) por campo.",
            "Eliminar duplicados por clave natural.",
            "Filtrado de valores imposibles (p.ej. demanda < 0).",
            "Escribir resultados en `/silver/...`.",
        ],
        refs=[],
    ),
    dict(
        title="Normalizar unidades (MW, kWh, €/MWh)",
        labels=["Silver"],
        context="Unificar unidades y documentar conversiones.",
        checklist=[
            "Checklist de columnas con unidades.",
            "Conversión y test de magnitudes.",
            "Actualizar diccionario de datos.",
        ],
        refs=[],
    ),
    dict(
        title="Mapear tecnologías (renovables / no renovables)",
        labels=["Silver"],
        context="Crear dimensión/tabla de mapeo de tecnologías con flags.",
        checklist=[
            "Definir tabla `dim_tecnologia` con tipo y grupo.",
            "Join con generación por tecnología.",
            "Validar totales por hora.",
        ],
        refs=[],
    ),
    dict(
        title="Factores de emisión CO₂ por tecnología",
        labels=["Silver"],
        context="Asignar factores gCO2/kWh a cada tecnología para estimar emisiones.",
        checklist=[
            "Tabla `factor_emision` con fuente citada.",
            "Calcular `co2eq_g_per_kwh` por hora.",
            "Escribir `/silver/emisiones/`.",
        ],
        refs=["Fuente oficial/estándar de factores (documentar en `/docs/emisiones.md`)"],
    ),
    dict(
        title="Integrar demanda + generación + precios",
        labels=["Silver"],
        context="Tabla integrada por hora con métricas clave.",
        checklist=[
            "Diseñar esquema integrado (wide/long).",
            "Join y validación de latencias/coberturas.",
            "Persistir `/silver/integrado/`.",
        ],
        refs=[],
    ),
    dict(
        title="Data checks (expectativas básicas)",
        labels=["Silver"],
        context="Conjunto de expectativas (conteos, rangos) sobre Silver.",
        checklist=[
            "Definir expectativas (ej. `precio >= 0`).",
            "Script de validación reutilizable.",
            "Badge/markdown con estado de calidad.",
        ],
        refs=[],
    ),
    dict(
        title="EDA: curvas de demanda y mix",
        labels=["Analysis"],
        context="Exploración de curvas de demanda y distribución del mix.",
        checklist=[
            "Gráficos de líneas por hora/día.",
            "Agregados por día de semana/mes.",
            "Insights preliminares documentados.",
        ],
        refs=[],
    ),
    dict(
        title="EDA: precios y correlaciones",
        labels=["Analysis"],
        context="Explorar precios y su relación con demanda/renovables.",
        checklist=[
            "Series de precios (diaria/horaria).",
            "Correlaciones Pearson/Spearman.",
            "Interpretación y posibles hipótesis.",
        ],
        refs=[],
    ),
    dict(
        title="Detectar outliers/eventos (apagón 2025)",
        labels=["Analysis"],
        context="Identificación de eventos y outliers notables en series.",
        checklist=[
            "Definir rule-based o z-score/IQR.",
            "Marcar evento apagón 2025.",
            "Notebook con narrativa del hallazgo.",
        ],
        refs=[],
    ),
    dict(
        title="Gráficos exploratorios (notebooks)",
        labels=["Analysis"],
        context="Consolidar visualizaciones clave en notebooks reproducibles.",
        checklist=[
            "Notebook `nb-eda-overview.ipynb`.",
            "Guardar imágenes en `/docs/img/`.",
            "Notas de interpretación.",
        ],
        refs=[],
    ),
    dict(
        title="Informe EDA con hallazgos",
        labels=["Docs"],
        context="Documento resumido con hallazgos EDA.",
        checklist=[
            "Crear `/docs/eda_report.md`.",
            "Incluir gráficos y conclusiones.",
            "Lista de hipótesis a validar.",
        ],
        refs=[],
    ),
    dict(
        title="(Opcional) Modelo sencillo de demanda (ARIMA/regresión)",
        labels=["Analysis"],
        context="PoC de forecast de demanda corto plazo.",
        checklist=[
            "Baseline naive y modelo simple.",
            "Backtesting básico (split temporal).",
            "Métricas y comparativa.",
        ],
        refs=[],
    ),
]

SPRINT3_ISSUES = [
    dict(
        title="Definir KPIs (demanda, mix, precios, CO₂)",
        labels=["Gold"],
        context="Lista de KPIs y definiciones (medidas, granularidad, filtros).",
        checklist=[
            "Documento `/docs/kpis.md`.",
            "Alineación con stakeholders.",
            "Checklist de disponibilidad en Silver.",
        ],
        refs=[],
    ),
    dict(
        title="Modelado tablas Gold (agregaciones, dims)",
        labels=["Gold"],
        context="Crear tablas Gold optimizadas para BI.",
        checklist=[
            "Diseño de modelo (star-like si aplica).",
            "Agregados diarios/mensuales.",
            "Validación contra Silver.",
        ],
        refs=[],
    ),
    dict(
        title="Dataset PBIX (conexión a Gold)",
        labels=["Dashboard"],
        context="Configurar dataset y conexión directa a Gold.",
        checklist=[
            "Conectar Power BI → Gold.",
            "Campos, relaciones y medidas DAX.",
            "Publicación en Workspace.",
        ],
        refs=[],
    ),
    dict(
        title="Dashboard: visión general",
        labels=["Dashboard"],
        context="Página de overview con KPIs principales.",
        checklist=[
            "Tiles: demanda, renovables %, precio, CO₂.",
            "Tendencias y selector de rango.",
            "Diseño limpio y responsivo.",
        ],
        refs=[],
    ),
    dict(
        title="Dashboard: demanda vs. generación",
        labels=["Dashboard"],
        context="Página con curvas comparadas y mix por tecnología.",
        checklist=[
            "Líneas demanda vs generación.",
            "Áreas apiladas por tecnología.",
            "Segmentadores por día/tecnología.",
        ],
        refs=[],
    ),
    dict(
        title="Dashboard: precios y emisiones",
        labels=["Dashboard"],
        context="Página con series de precio y CO₂eq.",
        checklist=[
            "Línea de precios y CO₂eq.",
            "Tabla por día con medias y máximos.",
            "Tooltip con metadatos.",
        ],
        refs=[],
    ),
    dict(
        title="Filtros, segmentaciones, y performance",
        labels=["Dashboard"],
        context="Optimizar navegación y tiempos de carga.",
        checklist=[
            "Definir filtros globales.",
            "Reducir cardinalidad si es necesario.",
            "Revisar agregaciones/medidas costosas.",
        ],
        refs=[],
    ),
    dict(
        title="Testing con usuarios y ajustes",
        labels=["Dashboard"],
        context="Sesión de prueba con feedback y mejoras.",
        checklist=[
            "Plan de test corto (tareas).",
            "Recoger feedback (formulario).",
            "Aplicar cambios priorizados.",
        ],
        refs=[],
    ),
    dict(
        title="Documentación técnica y runbook",
        labels=["Docs"],
        context="Manual técnico y operativa (runbook).",
        checklist=[
            "`/docs/arquitectura.md` (Medallion + Fabric).",
            "`/docs/runbook_operacion.md`.",
            "FAQ y troubleshooting.",
        ],
        refs=[],
    ),
    dict(
        title="Preparar presentación final + guion demo",
        labels=["Docs"],
        context="Diapositivas y guion de la demo final.",
        checklist=[
            "Storyline (problema → solución).",
            "Capturas de dashboards.",
            "Ensayo con tiempos.",
        ],
        refs=[],
    ),
    dict(
        title="Retrospectiva proyecto",
        labels=["Docs"],
        context="Retro del equipo con acciones de mejora.",
        checklist=[
            "Qué fue bien / mal / mejoras.",
            "Acciones y responsables.",
            "Publicar conclusiones.",
        ],
        refs=[],
    ),
    # ---- OPTIONAL (Multiagente) ----
    dict(
        title="(Optional) Diseño agentes (Renovable, Fósil, Nuclear, Hidráulico, Supervisor)",
        labels=["Optional"],
        context="Definir responsabilidades y entradas/salidas de cada agente.",
        checklist=[
            "Diagrama de interacción.",
            "Inputs y outputs por agente.",
            "Condiciones y límites (reglas).",
        ],
        refs=[],
    ),
    dict(
        title="(Optional) Reglas de coordinación (if-then) en Python",
        labels=["Optional"],
        context="Implementar lógica simple de coordinación con reglas deterministas.",
        checklist=[
            "Plantilla `agents/` con clases y `run()`.",
            "Reglas `if-then` priorizando renovables.",
            "Logger de decisiones.",
        ],
        refs=[],
    ),
    dict(
        title="(Optional) Simulaciones con escenarios históricos",
        labels=["Optional"],
        context="Probar el MAS con casos reales (incluyendo apagón 2025).",
        checklist=[
            "Loader de escenarios (Silver/Gold).",
            "Métricas de cobertura y coste.",
            "Resultados comparados vs baseline.",
        ],
        refs=[],
    ),
    dict(
        title="(Optional) Informe MAS",
        labels=["Optional"],
        context="Documento con resultados de simulaciones, límites y futuro trabajo.",
        checklist=[
            "Metodología y reglas.",
            "Resultados cuantitativos.",
            "Roadmap a ML multiagente.",
        ],
        refs=[],
    ),
]

ALL_SPRINTS = [
    ("Sprint 1 — Bronze", SPRINT1_ISSUES),
    ("Sprint 2 — Silver + EDA", SPRINT2_ISSUES),
    ("Sprint 3 — Gold + Dashboards + Docs + Optional", SPRINT3_ISSUES),
]

# ========= GITHUB OPS =========
def ensure_label(repo, name, color):
    try:
        repo.get_label(name)
        print(f"Etiqueta ya existía: {name}")
    except GithubException:
        repo.create_label(name=name, color=color)
        print(f"Etiqueta creada: {name}")

def ensure_milestone(repo, title, description, due_on):
    # opcional: normalizar microsegundos
    if due_on:
        due_on = due_on.replace(microsecond=0)

    for m in repo.get_milestones(state="open"):
        if m.title == title:
            print(f"Milestone ya existía: {title}")
            return m
        
    return repo.create_milestone(
        title=title,
        state="open",
        description=description,
        due_on=due_on if due_on else None
    )

def issue_exists(repo, title):
    for i in repo.get_issues(state="open"):
        if i.title.strip().lower() == title.strip().lower():
            return i
    return None

def main():
    if not GITHUB_TOKEN:
        raise SystemExit("Falta GITHUB_TOKEN en el entorno.")
    gh = Github(auth=Auth.Token(GITHUB_TOKEN))
    repo = gh.get_repo(REPO_FULL_NAME)

    # 1) Labels
    for name, color in LABELS:
        ensure_label(repo, name, color)

    # 2) Milestones
    milestone_objs = {}
    for cfg in MILESTONES_CFG:
        m = ensure_milestone(repo, cfg["title"], cfg["description"], cfg.get("due_on"))
        milestone_objs[cfg["title"]] = m

    # 3) Issues
    created_urls = []
    for sprint_title, issues in ALL_SPRINTS:
        milestone = milestone_objs[sprint_title]
        for it in issues:
            title = it["title"]
            labels = it["labels"]
            context = it.get("context", "")
            checklist = it.get("checklist", [])
            refs = it.get("refs", [])
            dod = it.get("dod", None)

            if issue_exists(repo, title):
                print(f"Ya hay un issue con ese título: {title}")
                continue

            body = build_body(
                sprint=sprint_title,
                context=context,
                checklist=checklist,
                refs=refs,
                dod=dod
            )
            issue = repo.create_issue(
                title=title,
                body=body,
                milestone=milestone,
                labels=[repo.get_label(l) for l in labels if l]
            )
            print(f"Creado: #{issue.number} {issue.title} → {issue.html_url}")
            created_urls.append(issue.html_url)

    if created_urls:
        output_path = os.path.join(os.path.dirname(__file__), "created_issues.txt")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(created_urls))
        print(f"Listado guardado en {output_path}")

if __name__ == "__main__":
    main()