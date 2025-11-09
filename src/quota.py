import os
import json
from datetime import datetime
from typing import Tuple

QUOTA_PATH_DEFAULT = "data/api_quota.json"

def _load_quota(path: str = QUOTA_PATH_DEFAULT) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"date": None, "count": 0}
    return {"date": None, "count": 0}

def _save_quota(state: dict, path: str = QUOTA_PATH_DEFAULT) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def _today_str() -> str:
    # Usa fuso local; se quiser, pode usar America/Sao_Paulo
    return datetime.now().strftime("%Y-%m-%d")

def allow_request(kind: str = "default", max_per_day: int = 100, path: str = QUOTA_PATH_DEFAULT) -> bool:
    """
    Verifica e incrementa a cota diária (persistida). Retorna True se pode chamar, False se atingiu o limite.
    """
    state = _load_quota(path)
    today = _today_str()
    if state.get("date") != today:
        state = {"date": today, "count": 0, "last_kind": None}

    if int(state.get("count", 0)) >= int(max_per_day):
        return False

    state["count"] = int(state.get("count", 0)) + 1
    state["last_kind"] = kind
    _save_quota(state, path)
    return True

def remaining_quota_today(max_per_day: int = 100, path: str = QUOTA_PATH_DEFAULT) -> int:
    """
    Retorna o restante de cota de hoje (max - count). Se mudou o dia, reseta para max.
    """
    state = _load_quota(path)
    today = _today_str()
    if state.get("date") != today:
        return max_per_day
    used = int(state.get("count", 0))
    return max(0, int(max_per_day) - used)

def quota_state(path: str = QUOTA_PATH_DEFAULT) -> Tuple[str, int]:
    """
    Retorna (data_str, count_hoje).
    """
    state = _load_quota(path)
    return (state.get("date"), int(state.get("count", 0)))