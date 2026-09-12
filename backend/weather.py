"""Live meteorological ingestion from Open-Meteo.

Zero-placeholder rule: if a live call fails, the value is returned as
available=False (UI renders "Insufficient data") and the caller decides whether
a clearly-tagged demo-fallback series may be used for the demonstration run.
"""
import datetime as dt
import requests

BASE = "https://api.open-meteo.com/v1/forecast"

HOURLY = "temperature_2m,relative_humidity_2m,wind_speed_10m,shortwave_radiation"
DAILY = "temperature_2m_max,temperature_2m_min"


def _utcnow():
    return dt.datetime.now(dt.timezone.utc)


def fetch_ward(lat, lon, forecast_days=6):
    """Return hourly+ daily record for one location, or raise on failure."""
    r = requests.get(BASE, params={
        "latitude": round(lat, 4), "longitude": round(lon, 4),
        "hourly": HOURLY, "daily": DAILY,
        "timezone": "auto", "forecast_days": forecast_days,
        "past_days": 2,
    }, timeout=20)
    r.raise_for_status()
    return r.json()


def synth_weather(lat, lon, now=None):
    """Clearly-tagged OFFLINE FALLBACK series (used only if the live API is
    unreachable, so the demo still renders). Every consuming layer is tagged
    provenance='demo-fallback'; it is never mistaken for live ingestion."""
    import math
    now = now or _utcnow()
    start = now - dt.timedelta(hours=48)
    times, t2, rh, ws, sw = [], [], [], [], []
    for i in range(48 + 168):  # 2d past + 7d ahead
        t = start + dt.timedelta(hours=i)
        hour = t.hour
        amp = 4.0
        temp = 28.0 - amp * math.cos(2 * math.pi * (hour - 2) / 24)
        times.append(t.strftime("%Y-%m-%dT%H:%M"))
        t2.append(round(temp, 1))
        rh.append(round(72 + 8 * math.sin(2 * math.pi * (hour - 6) / 24), 1))
        ws.append(round(6 + 2 * math.sin(2 * math.pi * hour / 24), 1))
        sw.append(int(max(0, 700 * math.cos(2 * math.pi * (hour - 13) / 24))))
    return {
        "_provenance": "demo-fallback",
        "generationtime_utc": _utcnow().isoformat(),
        "hourly": {"time": times, "temperature_2m": t2,
                   "relative_humidity_2m": rh, "wind_speed_10m": ws,
                   "shortwave_radiation": sw},
        "daily": {"temperature_2m_max": [max(t2[i:i+24]) for i in range(48, len(t2), 24)][:6]},
    }


def fetch_many(centroids, max_workers=8, forecast_days=6):
    """Fetch weather for many ward centroids with a small thread pool.
    Returns {ward_id: record} for successes and {ward_id: error} for failures.
    """
    from concurrent.futures import ThreadPoolExecutor
    out = {}

    def one(item):
        wid, (lat, lon) = item
        try:
            return wid, ("ok", fetch_ward(lat, lon, forecast_days))
        except Exception as e:  # noqa
            return wid, ("err", f"{type(e).__name__}: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for wid, res in ex.map(one, centroids.items()):
            out[wid] = res
    return out
