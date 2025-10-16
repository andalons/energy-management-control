# Enerlytics

**Plataforma de Análisis Inteligente del Sistema Eléctrico Español**

> _"La verdadera inteligencia artificial comienza con datos en los que se puede confiar"_

---

## 🔌 Contexto

El 28 de abril de 2025, la Península Ibérica sufrió el apagón eléctrico más significativo de las últimas décadas. Un fallo en la red provocó la caída abrupta de la demanda a niveles técnicos de cero, afectando a millones de usuarios durante horas. Las causas exactas siguen sin esclarecerse completamente.

Enerlytics nació como una investigación forense de este evento, pero rápidamente se transformó en algo más ambicioso: **una plataforma integral de ingeniería de datos** que integra múltiples fuentes heterogéneas del sistema eléctrico español, construyendo la infraestructura necesaria para análisis profundos y confiables.

## 🎯 Objetivos

- **Integrar** datos de generación eléctrica, demanda, precios de mercado y variables climáticas
- **Implementar** un pipeline end-to-end siguiendo la arquitectura Medallion (Bronze → Silver → Gold)
- **Crear** un modelo dimensional optimizado para análisis mediante star schema
- **Desarrollar** dashboards interactivos en Power BI para visualización de métricas clave
- **Garantizar** la calidad, trazabilidad y reproducibilidad de todo el proceso

## 🏗️ Arquitectura

```
┌─────────────────┐
│ DATA SOURCES    │
│ REData, ESIOS   │
│ Open-Meteo      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ BRONZE LAYER    │ ← Datos crudos JSON/Delta
│ (Raw Data)      │   Sin transformaciones
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SILVER LAYER    │ ← Limpieza, deduplicación
│ (Curated Data)  │   Normalización temporal
└────────┬────────┘   Validación de calidad
         │
         ▼
┌─────────────────┐
│ GOLD LAYER      │ ← Modelo dimensional
│ (Business Data) │   Star schema optimizado
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ POWER BI        │ ← Dashboards interactivos
└─────────────────┘
```

## 🛠️ Stack Tecnológico

- **Microsoft Fabric**: Plataforma principal (Lakehouse, Notebooks PySpark, orquestación)
- **Delta Lake**: Almacenamiento con transacciones ACID y versionado
- **Power BI**: Visualización y reporting
- **Python/PySpark**: Procesamiento y transformación de datos
- **GitHub**: Control de versiones y gestión de proyecto (metodología Scrum/Agile)

## 📂 Estructura de carpetas

```markdown
energy-management-control/
├── .github/ # Configuración de GitHub
│ ├── CONTRIBUTING.md # Normas de contribución
│ └── pull_request_template.md
│
├── notebooks/ # Notebooks de análisis
│ ├── bronze/ # Ingesta de datos crudos
│ ├── silver/ # Limpieza y normalización
│ └── gold/ # KPIs y modelo dimensional
│
├── data/ # Datos locales (gitignored)
│
├── docs/ # Documentación técnica
│
├── requirements.txt # Dependencias Python
└── README.md # Este archivo
```

## 🔗 Relación Fabric ↔ GitHub

### Microsoft Fabric gestiona:

- **Lakehouse (OneLake)**: Almacenamiento en capas Bronze/Silver/Gold
- **Notebooks**: Extracción y procesamiento con PySpark/Python
- **Power BI**: Dashboards y visualización integrada

### GitHub se usa para:

- Versionado de código (notebooks exportados, scripts)
- Gestión de issues, milestones y Pull Requests
- Documentación técnica en `/docs`
- Automatización del backlog mediante GitHub Projects

> ⚠️ **Los datos NO se guardan en GitHub**, solo el código y documentación. Los datos viven en Fabric/OneLake.

## 📊 Fuentes de Datos

| Fuente         | API                                 | Datos Clave                                                          |
| -------------- | ----------------------------------- | -------------------------------------------------------------------- |
| **REData**     | Red Eléctrica de España             | Demanda, generación por tecnología, balance eléctrico, emisiones CO₂ |
| **ESIOS**      | Sistema de Información del Operador | Precios SPOT horarios, previsiones de demanda, métricas de balance   |
| **Open-Meteo** | Weather API                         | Temperatura, viento, precipitaciones, radiación solar                |

**Cobertura**: 20 comunidades autónomas + sistemas insulares (Canarias, Baleares) + territorios (Ceuta, Melilla)  
**Período**: Enero 2023 - Junio 2025 (mensual)

## 🚀 Ciclo de Datos (Medallion)

### Bronze → Ingesta

- Datos crudos desde APIs en formato JSON
- Metadata enriquecida con timestamps de captura
- Sistema de logging y control de duplicados

### Silver → Normalización

- Deduplicación mediante claves de negocio
- Corrección de zonas horarias (UTC → hora peninsular)
- Eliminación de campos decorativos y nulos
- Validación de integridad y análisis exploratorio

### Gold → Modelo Dimensional

- **Dimensiones**: `dim_date`, `dim_geography`, `dim_technology`
- **Hechos**: `fact_generation_month`
- Star schema optimizado para Power BI
- Particionamiento estratégico por año

## 🎨 Dashboards Disponibles

- **Generación y Demanda por Tecnología**: Balance interno y déficit/superávit energético
- **Distribución Geográfica**: Mix energético por comunidad autónoma
- **Evolución de Renovables**: Tendencias de solar, eólica e hidráulica
- **Análisis Climático**: Correlación temperatura-demanda, viento-eólica, precipitaciones-hidráulica
- **Análisis del Apagón de Abril 2025**: Comparativa con 2024 y descarte de causas climáticas

## 🧑‍💻 Contribución

- Ver [`.github/CONTRIBUTING.md`](.github/CONTRIBUTING.md)
- **Todas las ramas y commits deben estar en inglés**
- Cada PR debe estar vinculado a una issue
- Los notebooks se exportan desde Fabric como `.ipynb` para versionado en GitHub

## 📈 Resultados Clave

- ✅ Pipeline completo Bronze → Silver → Gold operativo
- ✅ Modelo dimensional con integridad referencial verificada
- ✅ Eliminación de 720 registros duplicados (3% del total)
- ✅ Corrección sistémica del problema de conversión de zonas horarias
- ✅ Dashboards interactivos con filtros dinámicos en Power BI

## 🔮 Trabajo Futuro

### Corto Plazo

- Completar integración de datos horarios de ESIOS
- Ampliar dashboards con análisis de precios de mercado
- Automatización completa de pipelines

### Medio Plazo

- Análisis predictivo de demanda con Machine Learning
- Sistema de alertas ante anomalías en el sistema
- Cálculo de emisiones CO₂ totales por comunidad autónoma

### Largo Plazo

- Sistema multiagente para simulación de gestión energética
- API propia para consumo externo de datos procesados
- Análisis forense detallado del apagón con datos subhorarios

## 📚 Documentación Técnica

Ver [`/docs`](./docs) para documentación detallada:

- [Pipeline REData](./docs/pipeline-redata.md)
- [Arquitectura Medallion](./docs/architecture.md)

## 🎓 Equipo

**Factoría F5 - Bootcamp de Inteligencia Artificial**  
Período: 15 septiembre - 17 octubre 2025

| Nombre          | Responsabilidad                                |
| --------------- | ---------------------------------------------- |
| Andrea Alonso   | REData - Red Eléctrica de España, Scrum Master |
| Anca Bacria     | OMIE y REData (Mercados)                       |
| Max Beltrán     | ESIOS - Datos del sistema eléctrico            |
| Fernando García | Orquestación de pipelines y Data Analytics     |
| Alla Haruty     | Open-Meteo - Datos climáticos                  |

**Tutor/Stakeholder**: Alfonso del Valle Lima (Microsoft)

---

## 📄 Licencia

Este proyecto ha sido desarrollado con fines académicos como parte del Bootcamp de IA de Factoría F5.

## 🔗 Referencias

- [Red Eléctrica de España - API REData](https://www.ree.es/es/apidatos)
- [ESIOS - Sistema de Información del Operador](https://www.esios.ree.es/)
- [Open-Meteo - Weather API](https://open-meteo.com/)
- [OMIE - Operador del Mercado Ibérico](https://www.omie.es/)

---

**Enerlytics** - _Transformando datos eléctricos en conocimiento confiable_
