import requests, pandas as pd, os, re, unicodedata
from datetime import date

# === 1) Coordenadas representativas por CCAA ===
ccaa_coords = {
    "Andalucía": (37.3891, -5.9845),
    "Aragón": (41.6488, -0.8891),
    "Principado de Asturias": (43.3619, -5.8494),
    "Illes Balears": (39.5696, 2.6502),
    "Canarias": (28.4636, -16.2518),
    "Cantabria": (43.4623, -3.80998),
    "Castilla y León": (41.6523, -4.7245),
    "Castilla-La Mancha": (39.8628, -4.0273),
    "Cataluña": (41.3874, 2.1686),
    "Comunitat Valenciana": (39.4699, -0.3763),
    "Extremadura": (38.8794, -6.9707),
    "Galicia": (42.8782, -8.5448),
    "La Rioja": (42.4627, -2.44499),
    "Comunidad de Madrid": (40.4168, -3.7038),
    "Región de Murcia": (37.9922, -1.1307),
    "Comunidad Foral de Navarra": (42.8125, -1.6458),
    "País Vasco": (42.8467, -2.6716),
    "Ceuta": (35.8894, -5.3213),
    "Melilla": (35.2923, -2.9381),
}

# === 2) Rango: 2023-01-01 a 2024-12-31 ===
start = date(2023, 1, 1)
end   = date(2024, 12, 31)

# === 3) Petición a Open-Meteo ===
lats = ",".join(str(v[0]) for v in ccaa_coords.values())
lons = ",".join(str(v[1]) for v in ccaa_coords.values())

params = {
    "latitude": lats,
    "longitude": lons,
    "start_date": start.isoformat(),
    "end_date": end.isoformat(),
    "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
    "timezone": "Europe/Madrid",
}
url = "https://archive-api.open-meteo.com/v1/archive"
r = requests.get(url, params=params, timeout=180)
r.raise_for_status()
data = r.json()
locations = data if isinstance(data, list) else data.get("locations", [data])

# === 4) Parseo diario ===
rows = []
for name, loc in zip(ccaa_coords.keys(), locations):
    daily = loc.get("daily", {})
    times = daily.get("time", [])
    for i, t in enumerate(times):
        rows.append({
            "comunidad": name,
            "fecha": t[:10],
            "tmax": daily.get("temperature_2m_max", [None]*len(times))[i],
            "tmin": daily.get("temperature_2m_min", [None]*len(times))[i],
            "tmed": daily.get("temperature_2m_mean", [None]*len(times))[i],
            "prec": daily.get("precipitation_sum", [None]*len(times))[i],
            "viento_max": daily.get("wind_speed_10m_max", [None]*len(times))[i],
        })

df = pd.DataFrame(rows)
df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
df = df[(df["fecha"] >= pd.Timestamp(start)) & (df["fecha"] <= pd.Timestamp(end))]

# === 5) Agregado mensual ===
df_mensual = (
    df.assign(
        mes=df["fecha"].dt.month,
        año=df["fecha"].dt.year
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

# --- redondear a enteros ---
num_cols = ["tmax", "tmin", "tmed", "prec", "viento_max"]
df_mensual[num_cols] = df_mensual[num_cols].round(0).astype("Int64")

# === 6) Guardar un archivo por comunidad ===
def slugify(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Za-z0-9_-]+", "_", text).strip("_").lower()

out_dir = f"brz_ccaa_mensual_2023-2024"
os.makedirs(out_dir, exist_ok=True)

for comunidad, g in df_mensual.groupby("comunidad"):
    g = g.drop(columns=["comunidad"])
    fname = f"brz_{slugify(comunidad)}_mensual_2023_2024.csv"
    path = os.path.join(out_dir, fname)
    g.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
    print("Guardado:", path)
