import requests, pandas as pd, os, re, unicodedata, time
from datetime import date

# ===== 1) Config =====
# Grupos de CCAA por petición para evitar 429
GROUP_SIZE = 6
# Retries ante 429/5xx
MAX_RETRIES = 6
INITIAL_BACKOFF = 1.5  # segundos

# ===== 2) Coordenadas por CCAA =====
ccaa_coords = {
    "Andalucía": (37.3891, -5.9845),
    "Aragón": (41.6488, -0.8891),
    "Asturias": (43.3619, -5.8494),
    "Baleares": (39.5696, 2.6502),
    "Canarias": (28.4636, -16.2518),
    "Cantabria": (43.4623, -3.80998),
    "Castilla León": (41.6523, -4.7245),
    "Castilla La Mancha": (39.8628, -4.0273),
    "Cataluña": (41.3874, 2.1686),
    "Valencia": (39.4699, -0.3763),
    "Extremadura": (38.8794, -6.9707),
    "Galicia": (42.8782, -8.5448),
    "Rioja": (42.4627, -2.44499),
    "Madrid": (40.4168, -3.7038),
    "Murcia": (37.9922, -1.1307),
    "Navarra": (42.8125, -1.6458),
    "País Vasco": (42.8467, -2.6716),
    "Ceuta": (35.8894, -5.3213),
    "Melilla": (35.2923, -2.9381),
}

# ===== 3) Rango: 2023-01-01 hasta hoy =====
start_global = date(2023, 1, 1)
end_global = date.today()
# ===== 4) Utilidades =====
def slugify(text: str) -> str:
    return re.sub(
        r"[^A-Za-z0-9_-]+", "_",
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    ).strip("_").lower()

def chunks(lst, n):
    """Particiona 'lst' en trozos de tamaño n."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_open_meteo(coords_items, start_date, end_date):
    """Llama al endpoint para un grupo de coords con reintentos/backoff; devuelve lista de 'locations'."""
    lats = ",".join(str(v[0]) for _, v in coords_items)
    lons = ",".join(str(v[1]) for _, v in coords_items)

    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
        "timezone": "Europe/Madrid",
    }
    url = "https://archive-api.open-meteo.com/v1/archive"

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, timeout=180)
        if resp.status_code == 200:
            data = resp.json()
            return data if isinstance(data, list) else data.get("locations", [data])

        # Si 429 u otro error recuperable, esperar (respeta Retry-After si viene)
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            time.sleep(wait)
            backoff *= 2
            continue

        # Otros errores: levantar excepción con detalle
        resp.raise_for_status()

    raise RuntimeError(f"Fallo tras {MAX_RETRIES} intentos (último status {resp.status_code})")

# ===== 5) Descarga por años y grupos para evitar 429 =====
rows = []
ccaa_list = list(ccaa_coords.items())

years = [2023, 2024, 2025]
for year in years:
    y_start = date(year, 1, 1)
    y_end = date(year, 12, 31)
    # recortar al rango global
    if y_end < start_global or y_start > end_global:
        continue
    y_start = max(y_start, start_global)
    y_end   = min(y_end, end_global)

    print(f"Descargando {year} ({y_start} a {y_end})...")
    for group in chunks(ccaa_list, GROUP_SIZE):
        # llamada con reintentos
        locations = fetch_open_meteo(group, y_start, y_end)
        # parsear
        for (name, _), loc in zip([*group], locations):
            daily = loc.get("daily", {})
            times = daily.get("time", [])
            tmax = daily.get("temperature_2m_max", [])
            tmin = daily.get("temperature_2m_min", [])
            tmed = daily.get("temperature_2m_mean", [])
            prec = daily.get("precipitation_sum", [])
            vmax = daily.get("wind_speed_10m_max", [])
            for i, t in enumerate(times):
                rows.append({
                    "comunidad": name,
                    "fecha": t[:10],
                    "tmax": tmax[i] if i < len(tmax) else None,
                    "tmin": tmin[i] if i < len(tmin) else None,
                    "tmed": tmed[i] if i < len(tmed) else None,
                    "prec": prec[i] if i < len(prec) else None,
                    "viento_max": vmax[i] if i < len(vmax) else None,
                })

# ===== 6) DataFrame, agregado mensual y redondeo =====
df = pd.DataFrame(rows)
if df.empty:
    raise SystemExit("No se recibieron datos.")

df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
df = df[(df["fecha"] >= pd.Timestamp(start_global)) & (df["fecha"] <= pd.Timestamp(end_global))]

df_mensual = (
    df.assign(
        año=df["fecha"].dt.year,
        mes=df["fecha"].dt.month  # 1..12
    )
    .groupby(["comunidad", "año", "mes"], as_index=False)
    .agg(
        tmax=("tmax", "mean"),
        tmin=("tmin", "mean"),
        tmed=("tmed", "mean"),
        prec=("prec", "sum"),
        viento_max=("viento_max", "mean"),
    )
    .sort_values(["comunidad", "año", "mes"])
)

num_cols = ["tmax", "tmin", "tmed", "prec", "viento_max"]
df_mensual[num_cols] = df_mensual[num_cols].round(0).astype("Int64")

# ===== 7) Se crea carpeta =====
base_dir = f"brz_ccaa_mensual_2023_2025"
os.makedirs(base_dir, exist_ok=True)

for year in years:
    y_df = df_mensual[df_mensual["año"] == year]
    if y_df.empty:
        continue
    year_dir = os.path.join(base_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)

    for comunidad, g in y_df.groupby("comunidad"):
        g = g.drop(columns=["comunidad", "año"])
        fname = f"brz_{slugify(comunidad)}_{year}.json"
        path = os.path.join(year_dir, fname)

        # exportamos a JSON (lista de registros)
        g.to_json(path, orient="records", force_ascii=False, indent=2)

        print("Guardado:", path)

print("¡Archivos JSON guardados en carpetas por CCAA y año!")
