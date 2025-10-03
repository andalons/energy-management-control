import requests, pandas as pd, os, re, unicodedata, time, random
from datetime import date, timedelta
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ===== 1) Config robusta =====
GROUP_SIZE = 3
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.5
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
PAUSE_BETWEEN_CALLS = 0.5  # s

# ===== 2) Coordenadas por TERRITORIO =====
territories_coords = {
    "Baleares": (39.5696, 2.6502),       # Palma
    "Canarias": (28.4636, -16.2518),     # Santa Cruz de Tenerife
    "Ceuta": (35.8894, -5.3213),
    "Melilla": (35.2923, -2.9381),
    "Península": (40.0, -4.0),           # centro aproximado peninsular
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
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def iter_months(d0: date, d1: date):
    """Genera pares (inicio_mes, fin_mes) dentro del rango [d0, d1]."""
    cur = date(d0.year, d0.month, 1)
    end = d1
    while cur <= end:
        if cur.month == 12:
            month_end = date(cur.year, 12, 31)
        else:
            month_end = date(cur.year, cur.month + 1, 1) - timedelta(days=1)
        s = max(cur, d0)
        e = min(month_end, d1)
        yield s, e
        cur = (month_end + timedelta(days=1))

def make_session():
    """Session con retries a nivel HTTP(S)."""
    sess = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

session = make_session()

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
        try:
            resp = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else data.get("locations", [data])

            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                wait = wait * (1.0 + random.random() * 0.25)  # jitter
                print(f"[{attempt}/{MAX_RETRIES}] status {resp.status_code}. Esperando {wait:.1f}s…")
                time.sleep(wait)
                backoff = min(backoff * 2.0, 60)
                continue

            resp.raise_for_status()

        except (requests.ReadTimeout, requests.ConnectTimeout) as e:
            wait = backoff * (1.0 + random.random() * 0.25)
            print(f"[{attempt}/{MAX_RETRIES}] timeout {e.__class__.__name__}. Esperando {wait:.1f}s…")
            time.sleep(wait)
            backoff = min(backoff * 2.0, 60)
            continue
        except requests.RequestException as e:
            wait = backoff * (1.0 + random.random() * 0.25)
            print(f"[{attempt}/{MAX_RETRIES}] error de red: {e}. Esperando {wait:.1f}s…")
            time.sleep(wait)
            backoff = min(backoff * 2.0, 60)
            continue

    raise RuntimeError(f"Fallo tras {MAX_RETRIES} intentos para {start_date}..{end_date} (grupo {len(coords_items)})")

# ===== 5) Descarga por MESES y acumulación DIARIA por territorio/año =====
territory_list = list(territories_coords.items())
years = [2023, 2024, 2025]

# Carpeta base en Lakehouse (BRONZE) — RUTA TERRITORIAL
base_dir = "./brz_territorial_diario_2023_2025"
os.makedirs(base_dir, exist_ok=True)

# Acumulador: dict[(year, territorio)] -> list[rows diarios]
bucket = {}

for year in years:
    y_start = max(date(year, 1, 1), start_global)
    y_end   = min(date(year, 12, 31), end_global)
    if y_start > y_end:
        # Año futuro sin datos (p. ej., si hoy está antes de fin de año)
        continue

    print(f"== Año {year} ==")
    for m_start, m_end in iter_months(y_start, y_end):
        print(f"  - Mes {m_start.strftime('%Y-%m')} ({m_start}..{m_end})")
        for group in chunks(territory_list, GROUP_SIZE):
            locations = fetch_open_meteo(group, m_start, m_end)
            time.sleep(PAUSE_BETWEEN_CALLS)

            for (name, _), loc in zip([*group], locations):
                daily = loc.get("daily", {})
                times = daily.get("time", [])
                tmax = daily.get("temperature_2m_max", [])
                tmin = daily.get("temperature_2m_min", [])
                tmed = daily.get("temperature_2m_mean", [])
                prec = daily.get("precipitation_sum", [])
                vmax = daily.get("wind_speed_10m_max", [])

                rows = []
                for i, t in enumerate(times):
                    rows.append({
                        "territorio": name,
                        "date": t[:10],
                        "temperature_2m_max": tmax[i] if i < len(tmax) else None,
                        "temperature_2m_min": tmin[i] if i < len(tmin) else None,
                        "temperature_2m_mean": tmed[i] if i < len(tmed) else None,
                        "precipitation_sum": prec[i] if i < len(prec) else None,
                        "wind_speed_10m_max": vmax[i] if i < len(vmax) else None,
                    })

                if not rows:
                    continue

                key = (year, name)
                bucket.setdefault(key, []).extend(rows)

# ===== 6) Escritura DIARIA por territorio/año =====
for year in years:
    year_dir = os.path.join(base_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)

    for territorio, _ in territory_list:
        key = (year, territorio)
        df = pd.DataFrame(bucket.get(key, []))

        # Ordenar por fecha y garantizar solo el año objetivo
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df[(df["date"].dt.year == year)]
            df = df.sort_values("date")
            # Convertimos a texto ISO (YYYY-MM-DD) para guardar limpio
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # Guardado crudo (nombres de API, sin transformaciones)
        fname = f"brz_{slugify(territorio)}_{year}.json"
        out_path = os.path.join(year_dir, fname)

        if df.empty:
            # Si no hubo datos (p. ej., año futuro completo), escribimos un array vacío
            with open(out_path, "w", encoding="utf-8") as f:
                f.write("[]\n")
        else:
            df.to_json(out_path, orient="records", force_ascii=False, indent=2)

print("¡Guardado en lkh_iemc/Files/bronze/AEMET/brz_territorial_diario_2023_2025/<año>/brz_<territorio>_<año>.json (DIARIO, SIN LIMPIEZA)!")
