# === Parameters ===
# MODO de ejecución
RUN_MODE = "incremental"      # "full" o "incremental"
DAYS_BACK = 5                 # relee últimos N días por seguridad
END_MODE = "today_minus_1"    # "today" o "today_minus_1"

BASE_DIR = "/lakehouse/default/Files/bronze/AEMET/brz_territorial_diario_2023_2025"

# Fuente y control
TIMEZONE = "Europe/Madrid"
GROUP_SIZE = 3
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.5
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
PAUSE_BETWEEN_CALLS = 0.5  # segundos

territories_coords = {
    "Baleares": (39.5696, 2.6502),
    "Canarias": (28.4636, -16.2518),
    "Ceuta": (35.8894, -5.3213),
    "Melilla": (35.2923, -2.9381),
    "Península": (40.0, -4.0),
}
import dbutils
import os, re, unicodedata, time, random, json, requests, pandas as pd
from datetime import date, timedelta, datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---------- Utilidades ----------
def slugify(text: str) -> str:
    return re.sub(
        r"[^A-Za-z0-9_-]+", "_",
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    ).strip("_").lower()

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def iter_months(d0: date, d1: date):
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
    sess = requests.Session()
    retry = Retry(
        total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
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

def fetch_open_meteo(coords_items, start_date, end_date, tz=TIMEZONE):
    lats = ",".join(str(v[0]) for _, v in coords_items)
    lons = ",".join(str(v[1]) for _, v in coords_items)
    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
        "timezone": tz,
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

def read_json_safe(path: str) -> pd.DataFrame:
    """Lee un JSON de registros; si no existe o está vacío, devuelve DF vacío con esquema correcto."""
    cols = ["territorio", "date", "temperature_2m_max", "temperature_2m_min",
            "temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max"]
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_json(path, orient="records", dtype=False)
        # normalizar esquema
        for c in cols:
            if c not in df.columns: df[c] = None
        # tipar fecha
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        return df[cols]
    except Exception as e:
        print(f"Advertencia: no se pudo leer {path}: {e}. Se asume DF vacío.")
        return pd.DataFrame(columns=cols)

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

# ---------- Determinar rango incremental ----------
territory_list = list(territories_coords.items())
today = date.today()
end_date = today - timedelta(days=1) if END_MODE == "today_minus_1" else today

FULL_START = date(2023, 1, 1)

def discover_start_date_from_json():
    """Para modo incremental, encuentra la fecha máxima existente en los JSON y aplica DAYS_BACK.
       Si no hay archivos, vuelve a FULL_START."""
    if RUN_MODE == "full":
        return FULL_START

    max_found = None
    # Revisa años desde 2023 hasta end_date.year (carpetas existentes)
    for y in range(FULL_START.year, end_date.year + 1):
        year_dir = os.path.join(BASE_DIR, str(y))
        if not os.path.isdir(year_dir):
            continue
        for territorio, _ in territory_list:
            fname = f"brz_{slugify(territorio)}_{y}.json"
            path = os.path.join(year_dir, fname)
            if not os.path.exists(path):
                continue
            df = read_json_safe(path)
            if df.empty: 
                continue
            m = df["date"].max()
            if pd.isna(m):
                continue
            m = m if isinstance(m, date) else pd.to_datetime(m).date()
            max_found = m if (max_found is None or m > max_found) else max_found

    if max_found is None:
        return FULL_START
    # retrocede DAYS_BACK
    return max(FULL_START, max_found - timedelta(days=DAYS_BACK))

start_date = discover_start_date_from_json()
if start_date > end_date:
    print(f"Sin trabajo: start_date={start_date} > end_date={end_date}")
    # si estás en Fabric con dbutils:
    _ = dbutils.notebook.exit("Sin trabajo") if 'dbutils' in globals() else None

print(f"Rango a procesar: {start_date} .. {end_date} (RUN_MODE={RUN_MODE}, DAYS_BACK={DAYS_BACK})")

# ---------- Descarga y acumulación (solo rango necesario) ----------
bucket = {}  # dict[(year, territorio)] -> list[rows]
for m_start, m_end in iter_months(start_date, end_date):
    print(f"Mes {m_start.strftime('%Y-%m')} ({m_start}..{m_end})")
    for group in chunks(territory_list, GROUP_SIZE):
        locations = fetch_open_meteo(group, m_start, m_end, tz=TIMEZONE)
        time.sleep(PAUSE_BETWEEN_CALLS)

        for (name, _), loc in zip([*group], locations):
            daily = loc.get("daily", {})
            times = daily.get("time", [])
            tmax = daily.get("temperature_2m_max", [])
            tmin = daily.get("temperature_2m_min", [])
            tmed = daily.get("temperature_2m_mean", [])
            prec = daily.get("precipitation_sum", [])
            vmax = daily.get("wind_speed_10m_max", [])

            for i, t in enumerate(times):
                d = t[:10]
                y = int(d[:4])
                row = {
                    "territorio": name,
                    "date": d,
                    "temperature_2m_max": tmax[i] if i < len(tmax) else None,
                    "temperature_2m_min": tmin[i] if i < len(tmin) else None,
                    "temperature_2m_mean": tmed[i] if i < len(tmed) else None,
                    "precipitation_sum": prec[i] if i < len(prec) else None,
                    "wind_speed_10m_max": vmax[i] if i < len(vmax) else None,
                }
                bucket.setdefault((y, name), []).append(row)

# ---------- Upsert en JSON por (año, territorio) ----------
# Regla: leer JSON existente del año/territorio -> concatenar nuevas filas en memoria -> drop_duplicates por 'date' (keep='last') -> ordenar -> escribir
for (year, territorio), rows in bucket.items():
    year_dir = os.path.join(BASE_DIR, str(year))
    ensure_dir(year_dir)
    fname = f"brz_{slugify(territorio)}_{year}.json"
    path = os.path.join(year_dir, fname)

    df_new = pd.DataFrame(rows)
    if not df_new.empty:
        df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce").dt.date

    df_old = read_json_safe(path)
    df_all = pd.concat([df_old, df_new], ignore_index=True)

    # conservar solo filas del año objetivo (por robustez, en caso de solapamientos)
    df_all = df_all[df_all["date"].apply(lambda x: isinstance(x, date) and x.year == year)]

    # upsert por fecha
    df_all = (df_all
              .sort_values(["date"])  # por si queremos keep='last' con orden temporal
              .drop_duplicates(subset=["date"], keep="last")
             )

    # salida bonita YYYY-MM-DD
    if not df_all.empty:
        df_all = df_all.sort_values("date")
        df_all["date"] = pd.to_datetime(df_all["date"]).dt.strftime("%Y-%m-%d")
        df_all.to_json(path, orient="records", force_ascii=False, indent=2)
    else:
        # si vacío, escribe array vacío (coherente con tu implementación)
        with open(path, "w", encoding="utf-8") as f:
            f.write("[]\n")

print("¡JSON actualizado en", BASE_DIR, "por año/territorio (incremental + upsert)!")

from notebookutils import mssparkutils
import json

# Aseguramos que siempre exista bucket
if 'bucket' not in globals():
    bucket = {}

# Calcula años tocados de forma segura
years_touched = list({year for (year, _) in bucket.keys()})

result = {
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "years_touched": years_touched
}

# result = json.dumps(result)

# Para verlo en logs
print("Returning to pipeline:")
print(result)

# Y este es el único valor que captura Fabric como `.output.result`
mssparkutils.notebook.exit(json.dumps(result))

