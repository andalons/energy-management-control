# Energy Management Control

Proyecto de análisis y visualización de datos energéticos.  
Basado en **Microsoft Fabric** como entorno principal de datos (OneLake, Notebooks, Power BI) y **GitHub** como repositorio de código y coordinación.

---

## 📂 Estructura de carpetas inicial

```markdown
energy-management-control/
├── .github/ # Configuración de GitHub
│ ├── CONTRIBUTING.md # Normas de contribución
│ └── pull_request_template.md # Plantilla de PR
│
├── scripts/ # Scripts auxiliares
│ └── automation/ # Automatización (issues, etc.)
│
├── notebooks/ # Notebooks de exploración y análisis
│ ├── bronze/ # Ingesta
│ ├── silver/ # Limpieza/enriquecimiento
│ └── gold/ # KPIs y dashboards
│
├── src/ # Código fuente principal
│ ├── api_clients/ # Clientes para REData, ESIOS, AEMET, OMIE
│ ├── pipelines/ # ETL, jobs de ingesta y procesamiento
│ └── utils/ # Funciones comunes (logging, config)
│
├── data/ # Datos locales (gitignored)
│
├── docs/ # Documentación del proyecto
│
├── requirements.txt # Dependencias Python
└── README.md # Descripción general del proyecto
```

---

## 🔗 Relación Fabric ↔ GitHub

- **Microsoft Fabric** gestiona:

  - Lakehouse (OneLake) → almacenamiento en capas **Bronze/Silver/Gold**.
  - Notebooks (PySpark/Python) → extracción y procesamiento.
  - Power BI (integrado en Fabric) → dashboards y visualización.

- **GitHub** se usa para:
  - Versionado de código (notebooks exportados, scripts auxiliares).
  - Gestión de issues, milestones y PRs.
  - Documentación (`docs/`).
  - Automatización de backlog y Project.

👉 **Los datos no se guardan en GitHub**, solo el código y documentación. Los datos viven en Fabric/OneLake.

---

## 🧑‍💻 Contribución

- Ver [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md).
- **Todas las ramas y commits deben estar en inglés.**
- Cada PR debe estar vinculado a una issue.

---

## 📊 Ciclo de datos (Medallion + Fabric)

- **Bronze** → ingesta bruta desde APIs (REData, ESIOS, AEMET, OMIE).
- **Silver** → datos limpios y enriquecidos (unidades, validaciones, CO₂).
- **Gold** → tablas optimizadas para KPIs y dashboards en Power BI.
