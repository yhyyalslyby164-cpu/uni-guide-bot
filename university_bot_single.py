# -*- coding: utf-8 -*-
"""
بوت تلغرام — المرشد الجامعي (نسخة ملف واحد)
==========================================
هذا الملف يحتوي كل شيء: البوت + طبقة قاعدة البيانات.
شغّله مباشرة دون أي مكتبات إضافية:

    BOT_TOKEN=التوكن python3 university_bot_single.py

للتخزين السحابي (البيانات لا تُفقد أبدًا) أضف:

    SUPABASE_URL=https://xxxx.supabase.co \
    SUPABASE_KEY=المفتاح \
    BOT_TOKEN=التوكن python3 university_bot_single.py

يجمع كل معلومات الجامعة: المعلومات العامة، الجداول، السنوات الأربع،
التخصصات في السنة الثالثة، والبحث عن المقررات — ويُدار كاملًا من داخل تلغرام.
"""

import html
import json
import os
import sqlite3
import urllib.error
import urllib.parse
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ==================== طبقة قاعدة البيانات ====================
# ------------------------------------------------------------ اختيار الوضع
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bot.db"),
)

TABLES = ("settings", "year_groups", "specializations", "courses")
# عمود المفتاح الأساسي لكل جدول (يُستخدم للتفريغ الكامل عبر REST)
PK = {
    "settings": "key",
    "year_groups": "year",
    "specializations": "id",
    "courses": "id",
}

SCHEMA_SQLITE = """
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS year_groups (
        year       INTEGER PRIMARY KEY,
        group_link TEXT NOT NULL DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS specializations (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        name       TEXT NOT NULL,
        group_link TEXT NOT NULL DEFAULT '',
        position   INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS courses (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        year      INTEGER NOT NULL,
        spec_id   INTEGER,
        name      TEXT NOT NULL,
        lectures  TEXT NOT NULL DEFAULT '',
        practical TEXT NOT NULL DEFAULT '',
        courses   TEXT NOT NULL DEFAULT '',
        files     TEXT NOT NULL DEFAULT '',
        position  INTEGER NOT NULL DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_courses_year ON courses(year, spec_id);
"""


# ======================================================= ذاكرة مؤقتة سريعة
_CACHE_TTL = float(os.getenv("DB_CACHE_TTL", "15"))
_CACHE = {}
_LOCK = threading.Lock()


def _cache_get(key):
    with _LOCK:
        item = _CACHE.get(key)
        if item and item[0] > time.time():
            return item[1]
    return None


def _cache_put(key, value):
    if _CACHE_TTL <= 0:
        return
    with _LOCK:
        _CACHE[key] = (time.time() + _CACHE_TTL, value)


def _cache_clear():
    with _LOCK:
        _CACHE.clear()


# ================================================== طبقة SQLite (محلي)
def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def _where_sql(filters):
    clauses, values = [], []
    for col, val in (filters or {}).items():
        if val is None:
            clauses.append("%s IS NULL" % col)
        else:
            clauses.append("%s = ?" % col)
            values.append(val)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", values


def _lite_select(table, filters=None, order=None, limit=None):
    where, values = _where_sql(filters)
    sql = "SELECT * FROM %s%s" % (table, where)
    if order:
        sql += " ORDER BY " + ", ".join("%s %s" % (c, d.upper()) for c, d in order)
    if limit:
        sql += " LIMIT %d" % int(limit)
    with _connect() as con:
        return [dict(r) for r in con.execute(sql, values)]


def _lite_insert(table, row):
    cols = list(row)
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (
        table, ", ".join(cols), ", ".join("?" * len(cols)))
    with _connect() as con:
        cur = con.execute(sql, [row[c] for c in cols])
        new_id = cur.lastrowid
    out = dict(row)
    if table in ("specializations", "courses"):
        out["id"] = new_id
    return out


def _lite_insert_many(table, rows):
    if not rows:
        return []
    cols = list(rows[0])
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (
        table, ", ".join(cols), ", ".join("?" * len(cols)))
    with _connect() as con:
        con.executemany(sql, [[r.get(c) for c in cols] for r in rows])
    return rows


def _lite_update(table, filters, row):
    where, values = _where_sql(filters)
    sets = ", ".join("%s = ?" % c for c in row)
    with _connect() as con:
        con.execute("UPDATE %s SET %s%s" % (table, sets, where),
                    list(row.values()) + values)


def _lite_delete(table, filters):
    where, values = _where_sql(filters)
    with _connect() as con:
        con.execute("DELETE FROM %s%s" % (table, where), values)


def _lite_delete_all(table):
    with _connect() as con:
        con.execute("DELETE FROM %s" % table)


def _lite_upsert(table, row, conflict):
    cols = list(row)
    sets = ", ".join("%s = excluded.%s" % (c, c) for c in cols)
    sql = ("INSERT INTO %s (%s) VALUES (%s) "
           "ON CONFLICT(%s) DO UPDATE SET %s") % (
        table, ", ".join(cols), ", ".join("?" * len(cols)),
        ", ".join(conflict), sets)
    with _connect() as con:
        con.execute(sql, [row[c] for c in cols])


# ================================================= طبقة Supabase (سحابي)
def _supa_request(method, path, params=None, body=None, prefer=None, retries=3):
    """نداء واحد إلى واجهة Supabase، مع إعادة المحاولة عند الأخطاء المؤقتة."""
    url = "%s/rest/v1/%s" % (SUPABASE_URL, path)
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": "Bearer %s" % SUPABASE_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    last = None
    for attempt in range(max(1, retries)):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else []
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            last = RuntimeError("Supabase %s %s -> %s %s" % (method, path, exc.code, detail))
            if exc.code < 500:
                raise last
        except Exception as exc:  # مشاكل شبكة مؤقتة
            last = exc
        if attempt < retries - 1:
            time.sleep(1)
    raise last if last else RuntimeError("Supabase request failed")


def _supa_filters(filters):
    params = {}
    for col, val in (filters or {}).items():
        params[col] = "is.null" if val is None else "eq.%s" % val
    return params


def _supa_select(table, filters=None, order=None, limit=None):
    params = {"select": "*"}
    params.update(_supa_filters(filters))
    if order:
        params["order"] = ",".join("%s.%s" % (c, d) for c, d in order)
    if limit:
        params["limit"] = str(int(limit))
    return _supa_request("GET", table, params=params) or []


def _supa_insert(table, row):
    data = _supa_request("POST", table, body=[row],
                         prefer="return=representation")
    if isinstance(data, list) and data:
        return data[0]
    return dict(row)


def _supa_insert_many(table, rows):
    if not rows:
        return []
    data = _supa_request("POST", table, body=list(rows),
                         prefer="return=representation")
    return data if isinstance(data, list) else list(rows)


def _supa_update(table, filters, row):
    _supa_request("PATCH", table, params=_supa_filters(filters), body=row)


def _supa_delete(table, filters):
    _supa_request("DELETE", table, params=_supa_filters(filters))


def _supa_delete_all(table):
    # Supabase يرفض الحذف بلا شرط، فنستخدم شرطًا يشمل كل الصفوف
    _supa_request("DELETE", table, params={PK[table]: "not.is.null"})


def _supa_upsert(table, row, conflict):
    _supa_request("POST", table, params={"on_conflict": ",".join(conflict)},
                  body=[row], prefer="resolution=merge-duplicates")


# =========================================== موزّع موحّد بين الوضعين
def _select(table, filters=None, order=None, limit=None):
    if USE_SUPABASE:
        return _supa_select(table, filters, order, limit)
    return _lite_select(table, filters, order, limit)


def _insert(table, row):
    if USE_SUPABASE:
        return _supa_insert(table, row)
    return _lite_insert(table, row)


def _insert_many(table, rows):
    if USE_SUPABASE:
        return _supa_insert_many(table, rows)
    return _lite_insert_many(table, rows)


def _update(table, filters, row):
    if USE_SUPABASE:
        return _supa_update(table, filters, row)
    return _lite_update(table, filters, row)


def _delete(table, filters):
    if USE_SUPABASE:
        return _supa_delete(table, filters)
    return _lite_delete(table, filters)


def _delete_all(table):
    if USE_SUPABASE:
        return _supa_delete_all(table)
    return _lite_delete_all(table)


def _upsert(table, row, conflict):
    if USE_SUPABASE:
        return _supa_upsert(table, row, conflict)
    return _lite_upsert(table, row, conflict)


# ================================================================ التهيئة
def storage_label():
    return "Supabase (سحابي)" if USE_SUPABASE else "SQLite (محلي)"


def init_db():
    """يهيّئ التخزين: يُنشئ الجداول محليًا، أو يتحقق من الجداول السحابية."""
    if USE_SUPABASE:
        missing = []
        for table in TABLES:
            try:
                _supa_select(table, limit=1)
            except Exception:
                missing.append(table)
        if missing:
            print("[supabase] ⚠️ جداول مفقودة:", ", ".join(missing))
            print("[supabase] افتح Supabase → SQL Editor والصق محتوى "
                  "supabase_schema.sql مرة واحدة، ثم أعد تشغيل البوت.")
        else:
            print("[supabase] ✅ متصل بقاعدة البيانات السحابية — البيانات محفوظة دائمًا")
        print("[supabase] عدد الصفوف الحالي:",
              {t: len(_supa_select(t, limit=1)) for t in TABLES} if not missing else "—")
        return

    with _connect() as con:
        con.executescript(SCHEMA_SQLITE)
    print("[sqlite] ✅ قاعدة البيانات المحلية جاهزة")


# ---------------------------------------------------------------- settings
def get_setting(key, default=""):
    cached = _cache_get(("setting", key))
    if cached is not None:
        return cached
    rows = _select("settings", {"key": key}, limit=1)
    value = rows[0]["value"] if rows else default
    _cache_put(("setting", key), value)
    return value


def set_setting(key, value):
    _upsert("settings", {"key": key, "value": value or ""}, ["key"])
    _cache_clear()


def all_settings():
    return {r["key"]: r["value"] for r in _select("settings")}


# -------------------------------------------------------------- year groups
def get_year_group(year):
    rows = _select("year_groups", {"year": int(year)}, limit=1)
    return rows[0]["group_link"] if rows else ""


def set_year_group(year, link):
    _upsert("year_groups", {"year": int(year), "group_link": link or ""}, ["year"])
    _cache_clear()


# ----------------------------------------------------------- specializations
def list_specs():
    return _select("specializations", order=[("position", "asc"), ("id", "asc")])


def get_spec(spec_id):
    rows = _select("specializations", {"id": int(spec_id)}, limit=1)
    return rows[0] if rows else None


def add_spec(name, group_link=""):
    row = _insert("specializations",
                  {"name": name, "group_link": group_link or "", "position": 0})
    _cache_clear()
    return row.get("id")


def update_spec(spec_id, **fields):
    if not fields:
        return
    _update("specializations", {"id": int(spec_id)}, dict(fields))
    _cache_clear()


def delete_spec(spec_id):
    _delete("courses", {"spec_id": int(spec_id)})
    _delete("specializations", {"id": int(spec_id)})
    _cache_clear()


# ------------------------------------------------------------------ courses
def list_courses(year, spec_id=None):
    filters = {"year": int(year),
               "spec_id": None if spec_id is None else int(spec_id)}
    return _select("courses", filters, order=[("position", "asc"), ("id", "asc")])


def get_course(course_id):
    rows = _select("courses", {"id": int(course_id)}, limit=1)
    return rows[0] if rows else None


def add_course(year, spec_id, name, **fields):
    row = {
        "year": int(year),
        "spec_id": None if spec_id is None else int(spec_id),
        "name": name,
        "lectures": fields.get("lectures", ""),
        "practical": fields.get("practical", ""),
        "courses": fields.get("courses", ""),
        "files": fields.get("files", ""),
        "position": 0,
    }
    out = _insert("courses", row)
    _cache_clear()
    return out.get("id")


def update_course(course_id, **fields):
    if not fields:
        return
    _update("courses", {"id": int(course_id)}, dict(fields))
    _cache_clear()


def delete_course(course_id):
    _delete("courses", {"id": int(course_id)})
    _cache_clear()


# ------------------------------------------------------- نسخ احتياطي / استعادة
def export_data():
    """يرجع كل المحتوى كقاموس قابل للحفظ في ملف JSON."""
    return {
        "version": 1,
        "settings": {r["key"]: r["value"] for r in _select("settings")},
        "year_groups": {str(r["year"]): r["group_link"]
                        for r in _select("year_groups")},
        "specializations": _select("specializations",
                                   order=[("position", "asc"), ("id", "asc")]),
        "courses": _select("courses", order=[("position", "asc"), ("id", "asc")]),
    }


def import_data(payload):
    """يستبدل كل المحتوى بمحتوى النسخة الاحتياطية. يرجع إحصاءً بما استُعيد."""
    if not isinstance(payload, dict):
        raise ValueError("صيغة النسخة غير صحيحة")

    settings = payload.get("settings") or {}
    year_groups = payload.get("year_groups") or {}
    specs = payload.get("specializations") or []
    courses = payload.get("courses") or []

    for table in TABLES:
        _delete_all(table)

    _insert_many("settings", [{"key": str(k), "value": str(v)}
                              for k, v in settings.items()])
    _insert_many("year_groups", [{"year": int(y), "group_link": g or ""}
                                 for y, g in year_groups.items()])

    # خريطة المعرّفات القديمة -> الجديدة
    id_map = {}
    for s in specs:
        row = _insert("specializations", {
            "name": s.get("name", "تخصص"),
            "group_link": s.get("group_link", ""),
            "position": s.get("position", 0),
        })
        id_map[s.get("id")] = row.get("id")

    new_courses = []
    for c in courses:
        old_spec = c.get("spec_id")
        new_spec = id_map.get(old_spec) if old_spec is not None else None
        new_courses.append({
            "year": c.get("year", 1),
            "spec_id": new_spec,
            "name": c.get("name", "مقرر"),
            "lectures": c.get("lectures", ""),
            "practical": c.get("practical", ""),
            "courses": c.get("courses", ""),
            "files": c.get("files", ""),
            "position": c.get("position", 0),
        })
    _insert_many("courses", new_courses)

    _cache_clear()
    return {"settings": len(settings), "year_groups": len(year_groups),
            "specializations": len(specs), "courses": len(courses)}


def is_empty():
    """هل لا يوجد أي محتوى بعد؟"""
    for table in TABLES:
        if _select(table, limit=1):
            return False
    return True

# --- جسر داخلي: يتيح استخدام db.* داخل نفس الملف ---
import sys as _sys
import types as _types
db = _sys.modules.get(__name__)
if db is None:  # حالة تحميل غير اعتيادي: ننشئ مساحة أسماء مطابقة
    db = _types.SimpleNamespace()
    for _k, _v in list(globals().items()):
        if _k.startswith('__') or _types.ModuleType is type(_v):
            continue
        if getattr(_v, '__module__', None) == __name__ or callable(_v):
            setattr(db, _k, _v)
    for _k in ('USE_SUPABASE', 'SUPABASE_URL', 'SUPABASE_KEY', 'DB_PATH',
               'TABLES', 'PK', 'SCHEMA_SQLITE', '_CACHE_TTL', '_CACHE', '_LOCK'):
        if _k in globals():
            setattr(db, _k, globals()[_k])
    _sys.modules[__name__] = db


# --------------------------------------------------------------------- config
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

# كشف عنوان الاستضافة تلقائيًا (Render / Railway / Koyeb) ليعمل الويب هوك بلا إعداد يدوي
if not WEBHOOK_URL:
    for _var in ("RENDER_EXTERNAL_URL", "RAILWAY_PUBLIC_DOMAIN", "KOYEB_PUBLIC_DOMAIN"):
        _val = os.getenv(_var, "").strip()
        if _val:
            WEBHOOK_URL = _val if _val.startswith("http") else "https://" + _val
            break
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "uniguide-secret")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram")
PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
DEFAULT_ADMIN_IDS = [
    int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()
]
API = "https://api.telegram.org/bot{token}/{method}"

YEARS = [1, 2, 3, 4]
YEAR_TITLES = {1: "السنة الأولى", 2: "السنة الثانية", 3: "السنة الثالثة", 4: "السنة الرابعة"}

# مسمّى كل حقل: (مفتاح التخزين, الاسم المعروض)
GENERAL_FIELDS = [
    ("university_name", "اسم الجامعة"),
    ("college_name", "الكلية أو التخصص العام"),
    ("calendar_link", "رابط التقويم الجامعي"),
    ("important_links", "روابط هامة"),
]
SCHEDULE_FIELDS = [
    ("lectures_schedule_link", "رابط جدول المحاضرات"),
    ("exams_schedule_link", "رابط جدول الامتحانات"),
]
COURSE_FIELDS = [
    ("lectures", "رابط المحاضرات"),
    ("practical", "رابط العملي"),
    ("courses", "رابط الدورات"),
    ("files", "رابط الملفات"),
]

# الذاكرة المؤقتة: من ينتظر إدخال قيمة، وماذا ينتظر بالضبط
PENDING = {}
ADMIN_STATE = set()


# ---------------------------------------------------------------- telegram API
def api(method, **params):
    url = API.format(token=BOT_TOKEN, method=method)
    data = json.dumps(params, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=70) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover
            if attempt == 2:
                print("[api error]", method, exc)
                return {"ok": False}
            time.sleep(2)


def send(chat_id, text, keyboard=None, preview=False):
    return api(
        "sendMessage",
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=not preview,
        reply_markup=json.dumps(keyboard, ensure_ascii=False) if keyboard else None,
    )


def answer(cq_id, text=None, alert=False):
    api("answerCallbackQuery", callback_query_id=cq_id, text=text or "", show_alert=alert)


# ----------------------------------------------------- ملفات (نسخ احتياطي)
def send_json_file(chat_id, filename, payload, caption=""):
    """يرسل قاموسًا كملف JSON مرفق ضمن رسالة تلغرام."""
    blob = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
    boundary = "----uniguide" + str(int(time.time()))
    parts = []

    def field(name, value):
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n".encode("utf-8"))

    field("chat_id", chat_id)
    if caption:
        field("caption", caption)
        field("parse_mode", "HTML")
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="document"; '
        f'filename="{filename}"\r\nContent-Type: application/json\r\n\r\n'.encode("utf-8"))
    parts.append(blob)
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    url = API.format(token=BOT_TOKEN, method="sendDocument")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(req, timeout=70) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover
        print("[sendDocument error]", exc)
        return {"ok": False}


def download_file(file_id):
    """ينزّل ملفًا من تلغرام ويرجع محتواه بايتات."""
    info = api("getFile", file_id=file_id)
    if not info.get("ok"):
        return None
    path = info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
    try:
        with urllib.request.urlopen(url, timeout=70) as resp:
            return resp.read()
    except Exception as exc:  # pragma: no cover
        print("[getFile error]", exc)
        return None


def restore_from_env():
    """استعادة تلقائية من متغير BACKUP_JSON عند الإقلاع (مفيدة للاستضافات التي تفقد الملفات)."""
    raw = os.getenv("BACKUP_JSON", "").strip()
    if not raw:
        return False
    try:
        stats = db.import_data(json.loads(raw))
    except Exception as exc:
        print("[restore error]", exc)
        return False
    print("[restore] تمت الاستعادة من BACKUP_JSON:", stats)
    return True


# --------------------------------------------------------------------- helpers
def esc(value):
    return html.escape(value or "")


def is_admin(uid):
    return uid in ADMIN_STATE or uid in DEFAULT_ADMIN_IDS


def btn(text, data):
    return {"text": text, "callback_data": data}


def links_block(pairs):
    """يبني سطور الروابط المتوفرة فقط."""
    lines = []
    for label, value in pairs:
        if value:
            lines.append(f"▫️ {label}: <a href='{esc(value)}'>{esc(value)}</a>")
    return "\n".join(lines)


# --------------------------------------------------------------------- screens
def main_menu():
    uni = db.get_setting("university_name")
    title = f"🎓 <b>{esc(uni)}</b>\n\n" if uni else "🎓 <b>المرشد الجامعي</b>\n\n"
    text = title + "مرحبًا بك! اختر القسم الذي تريده من القائمة التالية 👇"
    kb = {
        "inline_keyboard": [
            [btn("📢 المعلومات العامة والتعاميم", "gen")],
            [btn("📅 الجداول الدراسية والامتحانية", "sched")],
            [btn("📘 السنة الأولى", "y:1"), btn("📗 السنة الثانية", "y:2")],
            [btn("📙 السنة الثالثة", "y:3"), btn("📕 السنة الرابعة", "y:4")],
            [btn("🔎 بحث عن مقرر", "search")],
        ]
    }
    return text, kb


def general_screen():
    lines = ["📢 <b>المعلومات العامة والتعاميم</b>", ""]
    body = links_block([(label, db.get_setting(key)) for key, label in GENERAL_FIELDS])
    lines.append(body if body else "⚠️ لم تُضَف أي معلومات بعد.")
    kb = {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]}
    return "\n".join(lines), kb


def schedule_screen():
    lines = ["📅 <b>الجداول الدراسية والامتحانية</b>", ""]
    body = links_block([(label, db.get_setting(key)) for key, label in SCHEDULE_FIELDS])
    lines.append(body if body else "⚠️ لم تُضَف أي جداول بعد.")
    kb = {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]}
    return "\n".join(lines), kb


def year_screen(year):
    lines = [f"<b>{YEAR_TITLES[year]}</b>", ""]
    group = db.get_year_group(year)
    if group:
        lines.append(f"👥 <a href='{esc(group)}'>رابط مجموعة السنة</a>")
    else:
        lines.append("👥 رابط مجموعة السنة: غير متوفر")
    lines.append("")

    if year == 3:
        specs = db.list_specs()
        if not specs:
            lines.append("⚠️ لم تُضَف تخصصات بعد.")
            return "\n".join(lines), {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]}
        lines.append("🎯 <b>التخصصات:</b> اختر تخصصك لعرض مقرراته")
        rows = [[btn(f"🎯 {s['name']}", f"sp:{s['id']}")] for s in specs]
        rows.append([btn("⬅️ رجوع", "menu")])
        return "\n".join(lines), {"inline_keyboard": rows}

    courses = db.list_courses(year)
    if not courses:
        lines.append("⚠️ لم تُضَف مقررات بعد.")
    else:
        lines.append("📚 <b>المقررات:</b>")
        for i, c in enumerate(courses, 1):
            lines.append(f"{i}. {esc(c['name'])}")
    rows = [[btn(f"📖 {c['name']}", f"c:{c['id']}")] for c in courses]
    rows.append([btn("⬅️ رجوع", "menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def spec_screen(spec_id):
    spec = db.get_spec(spec_id)
    if not spec:
        return "⚠️ التخصص غير موجود.", {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]}
    lines = [f"🎯 <b>{esc(spec['name'])}</b>", ""]
    if spec["group_link"]:
        lines.append(f"👥 <a href='{esc(spec['group_link'])}'>رابط مجموعة التخصص</a>")
    else:
        lines.append("👥 رابط مجموعة التخصص: غير متوفر")
    lines.append("\n📚 <b>مقررات التخصص:</b>")
    courses = db.list_courses(3, spec_id)
    if not courses:
        lines.append("⚠️ لم تُضَف مقررات بعد.")
    else:
        for i, c in enumerate(courses, 1):
            lines.append(f"{i}. {esc(c['name'])}")
    rows = [[btn(f"📖 {c['name']}", f"c:{c['id']}")] for c in courses]
    rows.append([btn("⬅️ رجوع", "y:3")])
    return "\n".join(lines), {"inline_keyboard": rows}


def course_screen(course_id):
    c = db.get_course(course_id)
    if not c:
        return "⚠️ المقرر غير موجود.", {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]}
    lines = [f"📖 <b>{esc(c['name'])}</b>", ""]
    if c["spec_id"]:
        spec = db.get_spec(c["spec_id"])
        lines.append(f"🎯 التخصص: {esc(spec['name']) if spec else '-'}")
    else:
        lines.append(f"🗓 {YEAR_TITLES.get(c['year'], '-')}")
    lines.append("")
    body = links_block([(label, c[key]) for key, label in COURSE_FIELDS])
    lines.append(body if body else "⚠️ لم تُضَف روابط لهذا المقرر بعد.")
    back = f"sp:{c['spec_id']}" if c["spec_id"] else f"y:{c['year']}"
    return "\n".join(lines), {"inline_keyboard": [[btn("⬅️ رجوع", back)]]}


# ---------------------------------------------------------------- admin panel
def admin_menu():
    text = "🛠 <b>لوحة التحكم</b>\n\nاختر ما تريد تعديله:"
    kb = {
        "inline_keyboard": [
            [btn("📢 المعلومات العامة", "a:gen"), btn("📅 الجداول", "a:sched")],
            [btn("📘 الأولى", "a:y:1"), btn("📗 الثانية", "a:y:2")],
            [btn("📙 الثالثة", "a:y:3"), btn("📕 الرابعة", "a:y:4")],
            [btn("🎯 إدارة التخصصات", "a:specs")],
            [btn("💾 نسخة احتياطية", "a:backup"), btn("📥 استعادة نسخة", "a:restore")],
            [btn("👁 معاينة البوت", "menu")],
        ]
    }
    return text, kb


def admin_gen_screen():
    lines = ["📢 <b>تعديل المعلومات العامة</b>", ""]
    body = links_block([(label, db.get_setting(key)) for key, label in GENERAL_FIELDS])
    lines.append(body if body else "لا توجد بيانات.")
    rows = [[btn(f"✏️ {label}", f"a:set:{key}")] for key, label in GENERAL_FIELDS]
    rows.append([btn("⬅️ رجوع", "a:menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_sched_screen():
    lines = ["📅 <b>تعديل الجداول</b>", ""]
    body = links_block([(label, db.get_setting(key)) for key, label in SCHEDULE_FIELDS])
    lines.append(body if body else "لا توجد بيانات.")
    rows = [[btn(f"✏️ {label}", f"a:set:{key}")] for key, label in SCHEDULE_FIELDS]
    rows.append([btn("⬅️ رجوع", "a:menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_year_screen(year):
    rows = [[btn("🔗 تعديل رابط مجموعة السنة", f"a:setyg:{year}")]]
    if year == 3:
        for s in db.list_specs():
            rows.append([btn(f"🎯 {s['name']}", f"a:asp:{s['id']}")])
        rows.append([btn("➕ إضافة تخصص", "a:addsp")])
    else:
        for c in db.list_courses(year):
            rows.append([btn(f"📖 {c['name']}", f"a:ac:{c['id']}")])
        rows.append([btn("➕ إضافة مقرر", f"a:addc:{year}")])
    rows.append([btn("⬅️ رجوع", "a:menu")])
    return f"🛠 <b>{YEAR_TITLES[year]}</b>", {"inline_keyboard": rows}


def admin_spec_screen(spec_id):
    spec = db.get_spec(spec_id)
    if not spec:
        return "⚠️ غير موجود.", {"inline_keyboard": [[btn("⬅️ رجوع", "a:y:3")]]}
    courses = db.list_courses(3, spec_id)
    lines = [
        f"🎯 <b>{esc(spec['name'])}</b>",
        "",
        f"رابط المجموعة: {esc(spec['group_link']) or '—'}",
        f"عدد المقررات: {len(courses)}",
    ]
    kb = {
        "inline_keyboard": [
            [btn("✏️ الاسم", f"a:esn:{spec_id}"), btn("🔗 رابط المجموعة", f"a:esl:{spec_id}")],
            [btn("➕ إضافة مقرر", f"a:addc:3:{spec_id}")],
            *[[btn(f"📖 {c['name']}", f"a:ac:{c['id']}")] for c in courses],
            [btn("🗑 حذف التخصص", f"a:dsp:{spec_id}")],
            [btn("⬅️ رجوع", "a:y:3")],
        ]
    }
    return "\n".join(lines), kb


def admin_course_screen(course_id):
    c = db.get_course(course_id)
    if not c:
        return "⚠️ غير موجود.", {"inline_keyboard": [[btn("⬅️ رجوع", "a:menu")]]}
    lines = [f"📖 <b>{esc(c['name'])}</b>", ""]
    for key, label in COURSE_FIELDS:
        lines.append(f"{label}: {esc(c[key]) or '—'}")
    back = f"a:asp:{c['spec_id']}" if c["spec_id"] else f"a:y:{c['year']}"
    rows = [
        [btn("✏️ الاسم", f"a:ecn:{course_id}")],
        [btn(f"✏️ {label}", f"a:ecl:{course_id}:{key}") for key, label in COURSE_FIELDS[:2]],
        [btn(f"✏️ {label}", f"a:ecl:{course_id}:{key}") for key, label in COURSE_FIELDS[2:]],
        [btn("🗑 حذف المقرر", f"a:dc:{course_id}")],
        [btn("⬅️ رجوع", back)],
    ]
    return "\n".join(lines), {"inline_keyboard": rows}


def ask_for_value(chat_id, uid, action, prompt):
    PENDING[uid] = action
    send(
        chat_id,
        prompt + "\n\nأرسل القيمة الآن (أو /cancel للإلغاء، أو أرسل ‎-‎ للتفريغ).",
        {"inline_keyboard": [[btn("❌ إلغاء", "a:menu")]]},
    )


# ------------------------------------------------------------------ callbacks
def handle_callback(cq):
    uid = cq["from"]["id"]
    chat_id = cq["message"]["chat"]["id"]
    data = cq.get("data", "")
    answer(cq["id"])

    try:
        if data == "menu":
            send(chat_id, *main_menu())
        elif data == "gen":
            send(chat_id, *general_screen())
        elif data == "sched":
            send(chat_id, *schedule_screen())
        elif data == "search":
            PENDING[uid] = "__search__"
            send(chat_id, "🔎 أرسل اسم المقرر الذي تبحث عنه:")
        elif data.startswith("y:"):
            send(chat_id, *year_screen(int(data.split(":")[1])))
        elif data.startswith("sp:"):
            send(chat_id, *spec_screen(int(data.split(":")[1])))
        elif data.startswith("c:"):
            send(chat_id, *course_screen(int(data.split(":")[1])))
        elif data == "admin":
            if is_admin(uid):
                send(chat_id, *admin_menu())
            else:
                send(chat_id, "🔐 هذا القسم للمشرفين فقط.")
        elif data.startswith("a:"):
            if not is_admin(uid):
                send(chat_id, "🔐 هذا القسم للمشرفين فقط.")
                return
            admin_callback(chat_id, uid, data)
    except Exception as exc:  # pragma: no cover
        print("[callback error]", exc)


def admin_callback(chat_id, uid, data):
    parts = data.split(":")
    kind = parts[1]

    if kind == "menu":
        send(chat_id, *admin_menu())
    elif kind == "gen":
        send(chat_id, *admin_gen_screen())
    elif kind == "sched":
        send(chat_id, *admin_sched_screen())
    elif kind == "y":
        send(chat_id, *admin_year_screen(int(parts[2])))
    elif kind == "specs":
        send(chat_id, *admin_year_screen(3))
    elif kind == "asp":
        send(chat_id, *admin_spec_screen(int(parts[2])))
    elif kind == "ac":
        send(chat_id, *admin_course_screen(int(parts[2])))

    # ------------------------------------------------------------ set values
    elif kind == "set":
        key = parts[2]
        label = dict(GENERAL_FIELDS + SCHEDULE_FIELDS)[key]
        ask_for_value(chat_id, uid, ("setting", key), f"✏️ تعديل: <b>{label}</b>")
    elif kind == "setyg":
        year = int(parts[2])
        ask_for_value(chat_id, uid, ("year_group", year), f"✏️ تعديل رابط مجموعة {YEAR_TITLES[year]}")
    elif kind == "esn":
        ask_for_value(chat_id, uid, ("spec_name", int(parts[2])), "✏️ تعديل اسم التخصص")
    elif kind == "esl":
        ask_for_value(chat_id, uid, ("spec_link", int(parts[2])), "✏️ تعديل رابط مجموعة التخصص")

    # ------------------------------------------------------------ add entries
    elif kind == "addsp":
        PENDING[uid] = ("new_spec", None)
        send(chat_id, "➕ أرسل اسم التخصص الجديد:")
    elif kind == "addc":
        year = int(parts[2])
        spec_id = int(parts[3]) if len(parts) > 3 else None
        PENDING[uid] = ("new_course_name", (year, spec_id))
        send(chat_id, "➕ أرسل اسم المقرر الجديد:")

    elif kind == "backup":
        send_json_file(
            chat_id,
            f"university_backup_{time.strftime('%Y-%m-%d')}.json",
            db.export_data(),
            "💾 <b>نسخة احتياطية</b>\nاحفظ هذا الملف. لاستعادته: أرسله هنا بعد الضغط على «استعادة نسخة».",
        )
        send(chat_id, "ℹ️ يمكنك أيضًا حفظ الملف نفسه في متغيّر <code>BACKUP_JSON</code> لاستعادة تلقائية كل مرة يُعاد تشغيل البوت فيها.",
             {"inline_keyboard": [[btn("⬅️ رجوع", "a:menu")]]})
    elif kind == "restore":
        PENDING[uid] = ("restore_file", None)
        send(chat_id, "📥 أرسل الآن ملف النسخة الاحتياطية (JSON) الذي حفظته سابقًا:")
    # ------------------------------------------------------------ course edit
    elif kind == "ecn":
        ask_for_value(chat_id, uid, ("course_name", int(parts[2])), "✏️ تعديل اسم المقرر")
    elif kind == "ecl":
        cid, field = int(parts[2]), parts[3]
        label = dict(COURSE_FIELDS)[field]
        ask_for_value(chat_id, uid, ("course_link", (cid, field)), f"✏️ تعديل: {label}")
    elif kind == "dc":
        cid = int(parts[2])
        c = db.get_course(cid)
        db.delete_course(cid)
        send(chat_id, "✅ تم حذف المقرر.")
        if c:
            if c["spec_id"]:
                send(chat_id, *admin_spec_screen(c["spec_id"]))
            else:
                send(chat_id, *admin_year_screen(c["year"]))
    elif kind == "dsp":
        db.delete_spec(int(parts[2]))
        send(chat_id, "✅ تم حذف التخصص ومقرراته.")
        send(chat_id, *admin_year_screen(3))


# --------------------------------------------------------------- text handler
def year_label(course):
    if course.get("spec_id"):
        spec = db.get_spec(course["spec_id"])
        return spec["name"] if spec else "تخصص"
    return YEAR_TITLES.get(course["year"], "")


def search_courses(query):
    q = query.strip().lower()
    found = []
    for year in YEARS:
        pool = db.list_courses(year)
        for s in db.list_specs():
            pool += db.list_courses(year, s["id"])
        for c in pool:
            if q and q in c["name"].lower():
                found.append(c)
    return found[:20]


def handle_document(msg):
    """يستقبل ملف النسخة الاحتياطية من المشرف ويستعيد المحتوى."""
    uid = msg["from"]["id"]
    chat_id = msg["chat"]["id"]
    doc = msg.get("document") or {}

    if not is_admin(uid):
        send(chat_id, "🔐 غير مسموح.")
        return

    name = doc.get("file_name", "")
    if not (name.endswith(".json") or doc.get("mime_type") == "application/json"):
        send(chat_id, "⚠️ أرسل ملف JSON فقط (ملف النسخة الاحتياطية).")
        return

    raw = download_file(doc["file_id"])
    if raw is None:
        send(chat_id, "❌ تعذّر تنزيل الملف، حاول مرة أخرى.")
        return

    try:
        stats = db.import_data(json.loads(raw.decode("utf-8")))
    except Exception as exc:
        send(chat_id, f"❌ الملف غير صالح: {esc(str(exc))}")
        return

    send(
        chat_id,
        "✅ <b>تمت الاستعادة بنجاح!</b>\n"
        f"▫️ معلومات عامة: {stats['settings']}\n"
        f"▫️ مجموعات السنوات: {stats['year_groups']}\n"
        f"▫️ تخصصات: {stats['specializations']}\n"
        f"▫️ مقررات: {stats['courses']}",
        {"inline_keyboard": [[btn("🏠 القائمة الرئيسية", "menu"), btn("🛠 لوحة التحكم", "a:menu")]]},
    )


def handle_text(msg):
    uid = msg["from"]["id"]
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if text.startswith("/cancel"):
        PENDING.pop(uid, None)
        send(chat_id, "تم الإلغاء.", {"inline_keyboard": [[btn("🏠 القائمة", "menu")]]})
        return

    pending = PENDING.pop(uid, None)
    if not pending:
        send(chat_id, "استخدم القائمة 👇", {"inline_keyboard": [[btn("🏠 القائمة الرئيسية", "menu")]]})
        return

    if pending == "__search__":
        results = search_courses(text)
        if not results:
            send(chat_id, f"❌ لا توجد نتائج لـ «{esc(text)}».",
                 {"inline_keyboard": [[btn("⬅️ رجوع", "menu")]]})
            return
        rows = [[btn(f"📖 {r['name']} ({year_label(r)})", f"c:{r['id']}")] for r in results]
        rows.append([btn("⬅️ رجوع", "menu")])
        send(chat_id, f"🔎 نتائج البحث عن «{esc(text)}»:", {"inline_keyboard": rows})
        return

    if not is_admin(uid):
        send(chat_id, "🔐 غير مسموح.")
        return

    value = "" if text == "-" else text
    kind, meta = (pending if isinstance(pending, tuple) else (pending, None))

    if kind == "setting":
        db.set_setting(meta, value)
        send(chat_id, f"✅ تم الحفظ: {esc(value) or '—'}")
        if meta in dict(GENERAL_FIELDS):
            send(chat_id, *admin_gen_screen())
        else:
            send(chat_id, *admin_sched_screen())
    elif kind == "year_group":
        db.set_year_group(meta, value)
        send(chat_id, "✅ تم حفظ رابط المجموعة.")
        send(chat_id, *admin_year_screen(meta))
    elif kind == "spec_name":
        db.update_spec(meta, name=value or "تخصص")
        send(chat_id, "✅ تم تعديل الاسم.")
        send(chat_id, *admin_spec_screen(meta))
    elif kind == "spec_link":
        db.update_spec(meta, group_link=value)
        send(chat_id, "✅ تم حفظ الرابط.")
        send(chat_id, *admin_spec_screen(meta))
    elif kind == "new_spec":
        sid = db.add_spec(value or "تخصص جديد")
        send(chat_id, "✅ تمت إضافة التخصص.")
        send(chat_id, *admin_spec_screen(sid))
    elif kind == "new_course_name":
        year, spec_id = meta
        cid = db.add_course(year, spec_id, value or "مقرر جديد")
        send(chat_id, "✅ تمت إضافة المقرر.")
        send(chat_id, *admin_course_screen(cid))
    elif kind == "course_name":
        db.update_course(meta, name=value or "مقرر")
        send(chat_id, "✅ تم تعديل الاسم.")
        send(chat_id, *admin_course_screen(meta))
    elif kind == "course_link":
        cid, field = meta
        db.update_course(cid, **{field: value})
        send(chat_id, "✅ تم حفظ الرابط.")
        send(chat_id, *admin_course_screen(cid))


# --------------------------------------------------------------------- update
def help_text():
    return (
        "<b>مساعدة</b>\n\n"
        "/start — القائمة الرئيسية\n"
        "/admin — لوحة المشرف\n"
        "/id — عرض معرّفك\n"
        "/cancel — إلغاء أي إدخال\n\n"
        "تصفّح الأقسام من الأزرار: المعلومات العامة، الجداول، السنوات الأربع، والبحث.\n"
        "المشرف يمكنه إضافة الروابط وتعديلها وحذفها من لوحة التحكم دون لمس الكود."
    )


def handle_update(update):
    try:
        if "message" in update:
            msg = update["message"]
            uid = msg["from"]["id"]
            chat_id = msg["chat"]["id"]

            if msg.get("document"):
                handle_document(msg)
                return

            text = (msg.get("text") or "").strip()

            if text.startswith("/start"):
                send(chat_id, *main_menu())
            elif text.startswith("/help"):
                send(chat_id, help_text())
            elif text.startswith("/id"):
                send(chat_id, f"🆔 معرّفك: <code>{uid}</code>")
            elif text.startswith("/admin"):
                if is_admin(uid):
                    send(chat_id, *admin_menu())
                else:
                    PENDING[uid] = ("login", None)
                    send(chat_id, "🔐 أرسل كلمة مرور المشرف:")
            elif isinstance(PENDING.get(uid), tuple) and PENDING[uid][0] == "login":
                PENDING.pop(uid, None)
                if text == ADMIN_PASSWORD:
                    ADMIN_STATE.add(uid)
                    send(chat_id, "✅ تم تسجيل الدخول كمشرف.")
                    send(chat_id, *admin_menu())
                else:
                    send(chat_id, "❌ كلمة المرور غير صحيحة.")
            else:
                handle_text(msg)
        elif "callback_query" in update:
            handle_callback(update["callback_query"])
    except Exception as exc:  # pragma: no cover
        print("[update error]", exc)


# ------------------------------------------------------------------- webhook mode
def set_webhook():
    url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
    result = api(
        "setWebhook",
        url=url,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=False,
        allowed_updates=["message", "callback_query"],
    )
    print(f"🔗 تم ربط الويب هوك: {url} -> {result.get('description', result)}")


def delete_webhook():
    print("🧹 إزالة الويب هوك:", api("deleteWebhook", drop_pending_updates=False))


class WebhookHandler(BaseHTTPRequestHandler):
    def _reply(self, code, body=b"ok"):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/health", "/healthz"):
            self._reply(200, "ok".encode())
        else:
            self._reply(404, "not found".encode())

    def do_POST(self):
        if self.path != WEBHOOK_PATH:
            self._reply(404, "not found".encode())
            return
        if self.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            self._reply(403, "forbidden".encode())
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            update = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception as exc:
            print("[webhook parse error]", exc)
            self._reply(200)
            return
        threading.Thread(target=handle_update, args=(update,), daemon=True).start()
        self._reply(200)

    def log_message(self, fmt, *args):  # اجعل السجلز هادئة
        pass


def run_webhook():
    db.init_db()
    restore_from_env()
    set_webhook()
    server = ThreadingHTTPServer((HOST, PORT), WebhookHandler)
    print(f"🌐 البوت يعمل بوضع الويب هوك على {HOST}:{PORT}{WEBHOOK_PATH}")
    server.serve_forever()


def run_polling():
    db.init_db()
    restore_from_env()
    # إن كان هناك ويب هوك مسجَّل مسبقيًا فهو يمنع الاستقبال المباشر، لذا نزيله
    delete_webhook()
    print("🤖 البوت يعمل الآن بوضع الاستقبال المباشر... (Ctrl+C للإيقاف)")
    offset = None
    while True:
        try:
            resp = api("getUpdates", offset=offset, timeout=50)
            for update in resp.get("result", []):
                offset = update["update_id"] + 1
                handle_update(update)
        except KeyboardInterrupt:
            print("\nتم الإيقاف.")
            break
        except Exception as exc:  # pragma: no cover
            print("[loop error]", exc)
            time.sleep(3)


# ------------------------------------------------------------------- main loop
def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "⚠️ لم يتم ضبط BOT_TOKEN.\n"
            "شغّل البوت بالشكل: BOT_TOKEN=xxxx python bot.py"
        )
    if WEBHOOK_URL:
        run_webhook()
    else:
        run_polling()

if __name__ == "__main__":
    main()