# Arquitectura del Proyecto – Energy Management Control

Este documento describe la arquitectura técnica del proyecto, basada en el modelo **Medallion (Bronze → Silver → Gold)** y soportada en **Microsoft Fabric** para la gestión de datos y en **GitHub** para la coordinación y versionado de código.

---

## 🔗 Relación entre Fabric y GitHub

- **Microsoft Fabric**

  - **OneLake (Lakehouse)** → almacenamiento en capas de datos (Bronze, Silver, Gold).
  - **Notebooks** → ingesta, limpieza, enriquecimiento y análisis.
  - **Dataflows / Pipelines** → orquestación de procesos ETL y automatización.
  - **Power BI** (integrado en Fabric) → dashboards y visualización de KPIs.

- **GitHub**
  - **Repositorio de código** → scripts auxiliares, notebooks exportados, clientes de API.
  - **Gestión ágil** → issues, milestones y PRs.
  - **Documentación** → arquitectura, KPIs, guías de contribución.
  - **Automatización** → scripts para backlog y workflows.

👉 Los **datos nunca se guardan en GitHub**: permanecen en Fabric/OneLake. GitHub solo contiene **código y documentación**.

---

## 📊 Arquitectura de datos (Medallion)

```sql
          +----------------------+
          |   External APIs       |
          |  REData / ESIOS       |
          |  AEMET / OMIE         |
          +----------+------------+
                     |
                     v
            +------------------+
            |  Bronze Layer    |
            |  Raw ingestion   |
            |  - HTTP requests |
            |  - OMIE files    |
            |  - Batch marks   |
            |  - Idempotency   |
            +------------------+
                     |
                     v
            +------------------+
            |  Silver Layer    |
            |  Clean + enrich  |
            |  - Units (MW,€)  |
            |  - CO₂ factors   |
            |  - Data quality  |
            |  - Normalization |
            +------------------+
                     |
                     v
            +------------------+
            |   Gold Layer     |
            |   KPIs & Models  |
            |  - Demand curves |
            |  - Mix analysis  |
            |  - Price trends  |
            |  - Emissions     |
            +------------------+
                     |
                     v
           +----------------------+
           |  Dashboards (PowerBI)|
           |  - KPIs visualized   |
           |  - Drilldowns        |
           |  - Storytelling      |
           +----------------------+
```

---

## 📂 Integración con repositorio GitHub

- `/src/api_clients/` → Clientes Python para REData, ESIOS, AEMET y OMIE.
- `/src/pipelines/` → Scripts de ingesta y limpieza (ejecutados en Fabric notebooks).
- `/notebooks/bronze/` → Exportación de notebooks de ingesta en Fabric.
- `/notebooks/silver/` → Notebooks de limpieza y enriquecimiento.
- `/notebooks/gold/` → KPIs y modelos para dashboards.
- `/docs/` → Documentación de arquitectura, KPIs y dailies.

---

## 🧭 Flujo de trabajo

1. **Ingesta (Bronze)**

   - Notebooks en Fabric conectan a APIs (REData, ESIOS, AEMET, OMIE).
   - Datos guardados en formato Delta/Parquet en OneLake.
   - Uso de _batch marks_ e _idempotencia_.

2. **Procesamiento (Silver)**

   - Limpieza de duplicados y valores nulos.
   - Normalización de unidades y zonas horarias (CET/CEST).
   - Enriquecimiento con factores de emisión de CO₂.

3. **Agregación (Gold)**

   - Cálculo de KPIs (demanda, mix energético, precios, emisiones).
   - Tablas optimizadas para consumo en Power BI.

4. **Visualización**

   - Dashboards en Power BI dentro de Fabric.
   - Segmentación por fecha, tecnología, región.

5. **Coordinación**
   - Issues, PRs y milestones en GitHub.
   - Código versionado y compartido con el equipo.
   - Documentación centralizada en `/docs/`.
