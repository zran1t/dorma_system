# common/subject_resolver.py
# 只做兩件事：
#  1) resolve_inter_subjects(cfg, dept)     → 部門通訊處室用到的 subjects
#  2) resolve_init_subjects(cfg, kol_id)    → 初始化部門「科別通訊處室」用到的 subjects

from __future__ import annotations
from typing import Any, Dict, List


# ─────────────────────────────────────────────────────────────
# 共用：最小驗證 + 取 nats servers
# ─────────────────────────────────────────────────────────────

def _get_servers(cfg: Dict[str, Any]) -> List[str]:
    nats = cfg.get("nats") or {}
    servers = nats.get("servers")
    if not isinstance(servers, list) or not servers or not all(isinstance(s, str) for s in servers):
        raise ValueError("cfg.nats.servers 必須為非空的字串清單")
    return servers


# ─────────────────────────────────────────────────────────────
# inter（部門通訊處室）
# 來源：configs/channels/inter.yaml
# 用途：
#   - Publisher（API/其它部門 → 本部門）發「原始 subject」： inter.dpt.{dept}.comm.in
#   - Subscriber（本部門收件）訂閱 deliver：                 inter.dpt.{dept}.comm.in.deliver
#   - from_department_tpl（可選）：                           inter.dpt.{dept}.comm.out
# ─────────────────────────────────────────────────────────────
def resolve_inter_subjects(cfg_inter: Dict[str, Any], dept: str) -> Dict[str, Any]:
    if not isinstance(dept, str) or not dept.strip():
        raise ValueError("dept 必須為非空字串，例如 'initialization'")

    servers = _get_servers(cfg_inter)
    subjects = cfg_inter.get("subjects") or {}

    to_tpl = subjects.get("to_department_tpl")
    to_deliver_tpl = subjects.get("to_department_deliver")
    from_tpl = subjects.get("from_department_tpl")  # 可選

    if not (isinstance(to_tpl, str) and isinstance(to_deliver_tpl, str)):
        raise ValueError("inter.yaml 缺少 subjects.to_department_tpl / to_department_deliver")

    to_department = to_tpl.format(dept=dept)
    to_department_deliver = to_deliver_tpl.format(dept=dept)
    from_department = from_tpl.format(dept=dept) if isinstance(from_tpl, str) else None

    return {
        # 實用資訊
        "servers": servers,
        "dept": dept,

        # 部門通訊處室實際會用到的 subject
        "to_department": to_department,                     # 發佈到「本部門」的原始 subject（送入 JS）
        "to_department_deliver": to_department_deliver,     # 本部門訂閱的 deliver subject（顯式 ACK）
        "from_department": from_department,                 # 可選：本部門對外輸出（若有用）
    }


# ─────────────────────────────────────────────────────────────
# initialization（intra：部門 ↔ 科別）
# 來源：configs/channels/initialization.yaml
# 用途（固定初始化部門語意）：
#   - 部門→單一 kol 發：   init.dpt.initialization.kol.{kol}.comm.in
#   - kol 收 deliver：     init.dpt.initialization.kol.{kol}.comm.in.deliver
#   - kol→部門 回報：      init.dpt.initialization.kol.{kol}.comm.out
#   - 部門收匯聚 deliver： init.dpt.initialization.comm.in.deliver
#   - 部門廣播：           init.dpt.initialization.broadcast
# ─────────────────────────────────────────────────────────────
def resolve_init_subjects(cfg_init: Dict[str, Any], kol_id: str) -> Dict[str, Any]:
    if not isinstance(kol_id, str) or not kol_id.strip():
        raise ValueError("kol_id 必須為非空字串，例如 'kol'")

    servers = _get_servers(cfg_init)
    subjects = cfg_init.get("subjects") or {}

    # 必要固定鍵
    dept_comm_in = subjects.get("department_comm_in")
    dept_broadcast = subjects.get("department_broadcast")
    to_kol_tpl = subjects.get("to_kol_tpl")
    to_kol_deliver_tpl = subjects.get("to_kol_deliver")
    from_kol_tpl = subjects.get("from_kol_tpl")

    need = {
        "subjects.department_comm_in": dept_comm_in,
        "subjects.department_broadcast": dept_broadcast,
        "subjects.to_kol_tpl": to_kol_tpl,
        "subjects.to_kol_deliver": to_kol_deliver_tpl,
        "subjects.from_kol_tpl": from_kol_tpl,
    }
    for key, val in need.items():
        if not isinstance(val, str) or not val.strip():
            raise ValueError(f"initialization.yaml 缺少或格式錯誤：{key}")

    # 展開
    to_kol = to_kol_tpl.format(kol=kol_id)
    to_kol_deliver = to_kol_deliver_tpl.format(kol=kol_id)
    from_kol = from_kol_tpl.format(kol=kol_id)
    # 注意：consumer deliver_subject 是 '...comm.in.deliver'，直接在 department_comm_in 後加 '.deliver'
    dept_comm_in_deliver = f"{dept_comm_in}.deliver"

    return {
        # 實用資訊
        "servers": servers,
        "kol_id": kol_id,

        # 科別通訊處室（KOL 端）用到的 subject
        "to_kol": to_kol,                         # 部門→單一 KOL（publisher 用原始 subject）
        "to_kol_deliver": to_kol_deliver,         # KOL 端訂閱 deliver
        "from_kol": from_kol,                     # KOL→部門（上行）

        # 部門通訊處室用到的 subject（與 KOL 互通）
        "dept_comm_in_deliver": dept_comm_in_deliver,  # 部門訂閱（KOL 上行匯聚 deliver）
        "department_broadcast": dept_broadcast,        # 部門廣播（需要時用）
    }