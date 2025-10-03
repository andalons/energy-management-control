# === Parameters ===
RUN_MODE = "incremental"        # "full" o "incremental"
RERUN_MONTHS = 2                # re-procesa los últimos N meses (incluye el más reciente)
END_ANCHOR = "prev_month_end"   # "prev_month_end" (recomendado) o "today"

BASE_DIR = "/lakehouse/default/Files/bronze/AEMET/brz_ccaa_mensual_2023_2025"

# Fuente y control
TIMEZONE = "Europe/Madrid"
GROUP_SIZE = 3
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.5
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
PAUSE_BETWEEN_CALLS = 0.5  # s

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


import os, re, unicodedata, time, random, json, math
import requests, pandas as pd
from datetime import date, timedelta

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ---------- Utilidades generales ----------
def slugify(text: str) -> str:
    return re.sub(
        r"[^A-Za-z0-9_-]+","_",
        unicodedata.normalize("NFKD", text).encode("ascii","ignore").decode("ascii")
    ).strip("_").lower()

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def last_day_of_month(y, m):
    if m == 12:
        return date(y, 12, 31)
    return date(y, m+1, 1) - timedelta(days=1)

def add_months(d: date, months: int) -> date:
    # mueve una fecha por meses, manteniendo el día dentro del mes destino
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, last_day_of_month(y, m).day)
    return date(y, m, day)

def iter_months(d0: date, d1: date):
    """Genera pares (inicio_mes, fin_mes) dentro del rango [d0, d1]."""
    cur = date(d0.year, d0.month, 1)
    end = d1
    while cur <= end:
        m_end = last_day_of_month(cur.year, cur.month)
        s = max(cur, d0)
        e = min(m_end, d1)
        yield s, e
        cur = m_end + timedelta(days=1)

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

# ---------- Sesión HTTP con retries/backoff ----------
def make_session():
    sess = requests.Session()
    retry = Retry(
        total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
        backoff_factor=1.0,
        status_forcelist=(429,500,502,503,504),
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

            if resp.status_code in (429,500,502,503,504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                wait = wait*(1.0 + random.random()*0.25)  # jitter
                print(f"[{attempt}/{MAX_RETRIES}] status {resp.status_code}. Esperando {wait:.1f}s…")
                time.sleep(wait)
                backoff = min(backoff*2.0, 60)
                continue

            resp.raise_for_status()

        except (requests.ReadTimeout, requests.ConnectTimeout) as e:
            wait = backoff*(1.0 + random.random()*0.25)
            print(f"[{attempt}/{MAX_RETRIES}] timeout {e.__class__.__name__}. Esperando {wait:.1f}s…")
            time.sleep(wait)
            backoff = min(backoff*2.0, 60)
            continue
        except requests.RequestException as e:
            wait = backoff*(1.0 + random.random()*0.25)
            print(f"[{attempt}/{MAX_RETRIES}] error de red: {e}. Esperando {wait:.1f}s…")
            time.sleep(wait)
            backoff = min(backoff*2.0, 60)
            continue

    raise RuntimeError(f"Fallo tras {MAX_RETRIES} intentos: {start_date}..{end_date} (grupo {len(coords_items)})")

# ---------- Lectura mensual JSON segura ----------
def read_json_monthly(path: str) -> pd.DataFrame:
    cols = ["comunidad","year","month",
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "wind_speed_10m_max"]
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_json(path, orient="records", dtype=False)
        for c in cols:
            if c not in df.columns: df[c] = None
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
        return df[cols]
    except Exception as e:
        print(f"Advertencia: no se pudo leer {path}: {e}. Se asume DF vacío.")
        return pd.DataFrame(columns=cols)

# ---------- Descubrimiento incremental ----------
FULL_START = date(2023, 1, 1)
ccaa_list = list(ccaa_coords.items())
today = date.today()

if END_ANCHOR == "prev_month_end":
    first_of_this_month = date(today.year, today.month, 1)
    end_date = first_of_this_month - timedelta(days=1)
else:
    end_date = today

def discover_start_month_from_json() -> date:
    """Para RUN_MODE incremental, encuentra el (year, month) máximo presente en archivos y resta RERUN_MONTHS-1."""
    if RUN_MODE == "full":
        return FULL_START

    last_ym = None  # (year, month)
    # Explora años desde 2023 hasta end_date.year
    for y in range(FULL_START.year, end_date.year + 1):
        ydir = os.path.join(BASE_DIR, str(y))
        if not os.path.isdir(ydir):
            continue
        for comunidad, _ in ccaa_list:
            f = os.path.join(ydir, f"brz_{slugify(comunidad)}_{y}_mensual.json")
            if not os.path.exists(f):
                continue
            df = read_json_monthly(f)
            if df.empty: 
                continue
            # buscamos el mes máximo que tenga al menos una métrica (puede haber nulls)
            df_ok = df.dropna(subset=["month"])
            if df_ok.empty:
                continue
            ym = df_ok["year"].astype(int).astype(str) + df_ok["month"].astype(int).astype(str).str.zfill(2)
            # máximo lexicográfico equivale a máximo temporal
            idxmax = ym.idxmax()
            y_max = int(df_ok.loc[idxmax, "year"])
            m_max = int(df_ok.loc[idxmax, "month"])
            cand = (y_max, m_max)
            if (last_ym is None) or (cand > last_ym):
                last_ym = cand

    if last_ym is None:
        return FULL_START

    # Convertimos último (year, month) a fecha primer día y restamos RERUN_MONTHS-1
    last_d = date(last_ym[0], last_ym[1], 1)
    n_back = max(1, int(RERUN_MONTHS)) - 1
    start = add_months(last_d, -n_back)
    return max(FULL_START, start)

start_date = discover_start_month_from_json()

if start_date > end_date:
    print(f"Sin trabajo: start_date={start_date} > end_date={end_date}")
    # en Fabric usamos dbutils
    _ = dbutils.notebook.exit("Sin trabajo") if 'dbutils' in globals() else None

print(f"[Mensual] Rango a procesar (por días): {start_date} .. {end_date}  |  RERUN_MONTHS={RERUN_MONTHS}")

# ---------- Descarga diaria dentro del rango y acumulación ----------
bucket = {}  # dict[(year, comunidad)] -> list[rows diarios]

for m_start, m_end in iter_months(start_date, end_date):
    print(f"  - Mes {m_start.strftime('%Y-%m')} ({m_start}..{m_end})")
    for group in chunks(ccaa_list, GROUP_SIZE):
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
                bucket.setdefault((y, name), []).append({
                    "comunidad": name,
                    "date": d,
                    "temperature_2m_max": tmax[i] if i < len(tmax) else None,
                    "temperature_2m_min": tmin[i] if i < len(tmin) else None,
                    "temperature_2m_mean": tmed[i] if i < len(tmed) else None,
                    "precipitation_sum": prec[i] if i < len(prec) else None,
                    "wind_speed_10m_max": vmax[i] if i < len(vmax) else None,
                })

# ---------- Agregación mensual y escritura/upsert JSON ----------
metrics = ["temperature_2m_max","temperature_2m_min","temperature_2m_mean",
           "precipitation_sum","wind_speed_10m_max"]

affected_years = sorted(set(y for (y, _) in bucket.keys()))
for year in affected_years or [end_date.year]:  # si no hay bucket, igual aseguramos estructura del año actual
    year_dir = os.path.join(BASE_DIR, str(year))
    ensure_dir(year_dir)

    for comunidad, _ in ccaa_list:
        key = (year, comunidad)
        rows = bucket.get(key, [])
        df_daily = pd.DataFrame(rows)

        # --- monthly_new: solo meses del rango procesado en este run
        if not df_daily.empty:
            df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce")
            df_daily = df_daily.dropna(subset=["date"])
            df_daily["year"] = df_daily["date"].dt.year
            df_daily["month"] = df_daily["date"].dt.month

            agg_df = (df_daily.groupby(["comunidad","year","month"], as_index=False)
                      .agg({
                          "temperature_2m_max": "mean",
                          "temperature_2m_min": "mean",
                          "temperature_2m_mean": "mean",
                          "precipitation_sum": "sum",
                          "wind_speed_10m_max": "max",
                      }))
            monthly_new = agg_df[["comunidad","year","month"] + metrics]
        else:
            monthly_new = pd.DataFrame(columns=["comunidad","year","month"] + metrics)

        # --- monthly_old: lo que haya ya en el JSON del año
        fname = f"brz_{slugify(comunidad)}_{year}_mensual.json"
        path = os.path.join(year_dir, fname)
        monthly_old = read_json_monthly(path)

        # --- plantilla 12 meses
        tpl = pd.DataFrame({"comunidad":[comunidad]*12, "year":[year]*12, "month": list(range(1,13))})

        # --- combinamos: old -> base; new -> actualiza meses del rango
        if monthly_old.empty:
            base_idx = pd.DataFrame(index=tpl["month"], columns=metrics, dtype="float")
        else:
            base_idx = (monthly_old
                        .set_index("month")[metrics]
                        .reindex(tpl["month"])
                        .astype(float))

        if not monthly_new.empty:
            new_idx = (monthly_new
                       .set_index("month")[metrics]
                       .astype(float))
            base_idx.update(new_idx)  # sobrescribe meses recalculados

        monthly_out = tpl.copy()
        monthly_out[metrics] = base_idx.values  # respeta orden 1..12

        # --- escribir JSON (array de 12 objetos, ordenado por mes)
        monthly_out.to_json(path, orient="records", force_ascii=False, indent=2)

print(f"¡Actualización mensual completada en {BASE_DIR}!")
