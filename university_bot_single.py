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
import io
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# httpx اختياري: عند توفّره نستخدمه لإعادة استخدام الاتصالات (Keep-Alive)
# وهذا يقلّل زمن كل نداء كثيرًا. وإن لم يكن مثبّتًا يعمل البوت بـ urllib عادةً.
try:
    import httpx as _httpx
except Exception:  # pragma: no cover
    _httpx = None


# ==================== طبقة قاعدة البيانات ====================
# ------------------------------------------------------------ اختيار الوضع
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bot.db"),
)

TABLES = ("settings", "year_groups", "specializations", "courses", "students")
# عمود المفتاح الأساسي لكل جدول (يُستخدم للتفريغ الكامل عبر REST)
PK = {
    "settings": "key",
    "year_groups": "year",
    "specializations": "id",
    "courses": "id",
    "students": "id",
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
        theory    TEXT NOT NULL DEFAULT '',
        labs      TEXT NOT NULL DEFAULT '',
        past      TEXT NOT NULL DEFAULT '',
        position  INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS students (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    TEXT NOT NULL DEFAULT '',
        chat_id    TEXT NOT NULL DEFAULT '',
        full_name  TEXT NOT NULL DEFAULT '',
        uni_id     TEXT NOT NULL DEFAULT '',
        year       INTEGER NOT NULL DEFAULT 0,
        spec_id    INTEGER,
        username   TEXT NOT NULL DEFAULT '',
        joined_at  TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT ''
    );

    CREATE INDEX IF NOT EXISTS idx_courses_year ON courses(year, spec_id);
    CREATE INDEX IF NOT EXISTS idx_students_uid ON students(user_id);
"""


# ======================================================= ذاكرة مؤقتة سريعة
_CACHE_TTL = float(os.getenv("DB_CACHE_TTL", "300"))
_CACHE_PRELOAD = (os.getenv("DB_CACHE_PRELOAD", "1").strip().lower()
                  not in ("0", "false", "no", "off"))
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


def _cache_drop(key):
    with _LOCK:
        _CACHE.pop(key, None)


def preload_cache():
    """يحمّل البيانات الثابتة إلى الذاكرة مرة واحدة عند الإقلاع.

    بعد ذلك كل نقرات الزر تُخدَم من الذاكرة فورًا (بلا نداء شبكة)،
    وتُحدَّث تلقائيًا فقط عند قيام المشرف بأي تعديل."""
    if not _CACHE_PRELOAD:
        return
    started = time.time()
    try:
        all_settings()
        for year in YEARS:
            get_year_group(year)
            list_courses(year)
        list_courses(3)
        specs = list_specs()
        for s in specs:
            list_courses(3, s["id"])
        print("[cache] ✅ تم تحميل البيانات الثابتة مسبقًا (%.2f ث)" % (time.time() - started))
    except Exception as exc:  # لا نُفشل الإقلاع أبدًا بسبب الكاش
        print("[cache preload warn]", exc)


# ============================== طبقة اتصال مشتركة (مجمّع اتصالات)
# الهدف: عدم فتح اتصال TCP/TLS جديد مع كل ضغطة زر. نستخدم عميلًا واحدًا
# يعيد استخدام الاتصالات (Keep-Alive) ويشاركه كل الخيوط — آمن وخيطيًا.
_HTTP_CLIENT = None
_HTTP_LOCK = threading.Lock()
_HTTP_POOL = int(os.getenv("HTTP_POOL_SIZE", "40") or "40")
_HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30") or "30")


def _get_http_client():
    """يرجع عميل httpx مشتركًا (أو None إن لم تتوفر المكتبة)."""
    global _HTTP_CLIENT
    if _httpx is None:
        return None
    if _HTTP_CLIENT is None:
        with _HTTP_LOCK:
            if _HTTP_CLIENT is None:
                try:
                    limits = _httpx.Limits(
                        max_connections=_HTTP_POOL,
                        max_keepalive_connections=max(10, _HTTP_POOL // 2),
                        keepalive_expiry=30.0,
                    )
                    _HTTP_CLIENT = _httpx.Client(
                        timeout=_httpx.Timeout(_HTTP_TIMEOUT, connect=10.0),
                        limits=limits,
                        follow_redirects=True,
                        headers={"Connection": "keep-alive"},
                    )
                except Exception as exc:  # pragma: no cover
                    print("[http] تعذّر إنشاء عميل httpx، سنستخدم urllib:", exc)
                    _HTTP_CLIENT = None
    return _HTTP_CLIENT


def _http_perform(method, url, headers=None, content=None, timeout=None):
    """ينفّذ طلب HTTP واحد ويرجع (الحالة, النص). لا يرفع استثناءً على 4xx/5xx.

    يستخدم الاتصال المشترك إن توفّر httpx، وإلا يعود إلى urllib."""
    timeout = timeout or _HTTP_TIMEOUT
    client = _get_http_client()
    if client is not None:
        resp = client.request(method, url, headers=headers or None,
                              content=content, timeout=timeout)
        return resp.status_code, resp.text
    req = urllib.request.Request(url, data=content,
                                 headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return exc.code, body


def _http_perform_raw(method, url, headers=None, timeout=None):
    """مثل _http_perform لكن يعيد البايتات (لتنزيل الملفات)."""
    timeout = timeout or _HTTP_TIMEOUT
    client = _get_http_client()
    if client is not None:
        resp = client.request(method, url, headers=headers or None, timeout=timeout)
        return resp.status_code, resp.content
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, b""


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


# أعمدة أُضيفت لاحقًا: تُضاف تلقائيًا لقواعد البيانات القديمة بلا فقدان بيانات
_MIGRATE_COLUMNS = {
    "courses": [
        ("theory", "TEXT NOT NULL DEFAULT ''"),
        ("labs", "TEXT NOT NULL DEFAULT ''"),
        ("past", "TEXT NOT NULL DEFAULT ''"),
    ],
    "students": [
        ("user_id", "TEXT NOT NULL DEFAULT ''"),
        ("chat_id", "TEXT NOT NULL DEFAULT ''"),
        ("full_name", "TEXT NOT NULL DEFAULT ''"),
        ("uni_id", "TEXT NOT NULL DEFAULT ''"),
        ("year", "INTEGER NOT NULL DEFAULT 0"),
        ("spec_id", "INTEGER"),
        ("username", "TEXT NOT NULL DEFAULT ''"),
        ("joined_at", "TEXT NOT NULL DEFAULT ''"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    ],
}


def _lite_migrate():
    """يضيف أي عمود ناقص في قاعدة قديمة — آمن ويتجاهل الأخطاء."""
    with _connect() as con:
        for table, cols in _MIGRATE_COLUMNS.items():
            try:
                have = {r["name"] for r in con.execute("PRAGMA table_info(%s)" % table)}
            except Exception:
                continue
            if not have:
                continue
            for name, decl in cols:
                if name in have:
                    continue
                try:
                    con.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, name, decl))
                except Exception as exc:
                    print("[migrate skip]", table, name, exc)


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
    last = None
    for attempt in range(max(1, retries)):
        try:
            code, raw = _http_perform(method, url, headers=headers, content=data)
            if 200 <= code < 300:
                return json.loads(raw) if raw.strip() else []
            detail = (raw or "")[:300]
            last = RuntimeError("Supabase %s %s -> %s %s" % (method, path, code, detail))
            if code < 500:          # خطأ دائم (مفتاح/جدول): لا فائدة من إعادة المحاولة
                raise last
        except RuntimeError:
            raise
        except Exception as exc:    # مشاكل شبكة مؤقتة
            last = exc
        if attempt < retries - 1:
            time.sleep(0.4 * (attempt + 1))   # تراجع سريع بدل انتظار ثابت
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
        with _DIAG_LOCK:
            DIAG["storage_ok"] = not missing
            if missing:
                DIAG["storage_note"] = "جداول ناقصة: " + ", ".join(missing)
            else:
                DIAG["storage_note"] = "الاتصال سليم والبيانات محفوظة دائمًا"
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
    _lite_migrate()
    with _DIAG_LOCK:
        DIAG["storage_ok"] = None
        DIAG["storage_note"] = "تخزين مؤقت — يُمسح عند إعادة تشغيل الخدمة"
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
    """كل الإعدادات الظاهرة (بلا مفاتيح النظام الداخلية)."""
    hit = _cache_get(("all_settings",))
    if hit is not None:
        return hit
    data = {r["key"]: r["value"] for r in _select("settings")
            if not str(r["key"]).startswith("_pending_")}
    _cache_put(("all_settings",), data)
    return data


# -------------------------------------------------------------- year groups
def get_year_group(year):
    key = ("ygroup", int(year))
    hit = _cache_get(key)
    if hit is not None:
        return hit
    rows = _select("year_groups", {"year": int(year)}, limit=1)
    value = rows[0]["group_link"] if rows else ""
    _cache_put(key, value)
    return value


def set_year_group(year, link):
    _upsert("year_groups", {"year": int(year), "group_link": link or ""}, ["year"])
    _cache_clear()


# ----------------------------------------------------------- specializations
def list_specs():
    hit = _cache_get(("specs",))
    if hit is not None:
        return hit
    rows = _select("specializations", order=[("position", "asc"), ("id", "asc")])
    _cache_put(("specs",), rows)
    return rows


def get_spec(spec_id):
    key = ("spec", int(spec_id))
    hit = _cache_get(key)
    if hit is not None:
        return hit
    rows = _select("specializations", {"id": int(spec_id)}, limit=1)
    row = rows[0] if rows else None
    _cache_put(key, row)
    return row


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
    key = ("courses", int(year), None if spec_id is None else int(spec_id))
    hit = _cache_get(key)
    if hit is not None:
        return hit
    filters = {"year": int(year),
               "spec_id": None if spec_id is None else int(spec_id)}
    rows = _select("courses", filters, order=[("position", "asc"), ("id", "asc")])
    _cache_put(key, rows)
    return rows


def get_course(course_id):
    key = ("course", int(course_id))
    hit = _cache_get(key)
    if hit is not None:
        return hit
    rows = _select("courses", {"id": int(course_id)}, limit=1)
    row = rows[0] if rows else None
    _cache_put(key, row)
    return row


def add_course(year, spec_id, name, **fields):
    row = {
        "year": int(year),
        "spec_id": None if spec_id is None else int(spec_id),
        "name": name,
        "lectures": fields.get("lectures", ""),
        "practical": fields.get("practical", ""),
        "courses": fields.get("courses", ""),
        "files": fields.get("files", ""),
        "theory": fields.get("theory", ""),
        "labs": fields.get("labs", ""),
        "past": fields.get("past", ""),
        "position": 0,
    }
    out = _insert("courses", row)
    _cache_clear()
    return out.get("id")


# ------------------------------------------------------------------ students
def _now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_student(user_id):
    rows = _select("students", {"user_id": str(user_id)}, limit=1)
    return rows[0] if rows else None


def upsert_student(user_id, chat_id="", **fields):
    """ينشئ سجل الطالب أو يحدّثه — الشكل الوحيد الآمن في الوضعين."""
    row = {"user_id": str(user_id)}
    existing = get_student(user_id)
    if fields.get("full_name") is not None:
        row["full_name"] = (fields.get("full_name") or "")[:64]
    if fields.get("uni_id") is not None:
        row["uni_id"] = (fields.get("uni_id") or "")[:24]
    if fields.get("year") is not None:
        try:
            row["year"] = int(fields.get("year") or 0)
        except Exception:
            row["year"] = 0
    if fields.get("spec_id") is not None:
        row["spec_id"] = fields.get("spec_id")
    if fields.get("username") is not None:
        row["username"] = (fields.get("username") or "")[:64]
    if chat_id:
        row["chat_id"] = str(chat_id)
    row["updated_at"] = _now()
    if existing is None:
        row["joined_at"] = _now()
        _insert("students", row)
    else:
        _update("students", {"user_id": str(user_id)}, row)
    _cache_clear()
    return get_student(user_id)


def list_students(limit=None):
    return _select("students", order=[("id", "asc")], limit=limit)


def count_students():
    try:
        return len(_select("students", limit=1000))
    except Exception:
        return 0


def get_student_by_id(record_id):
    """يجلب سجل الطالب بمفتاحه الداخلي (id) — يُستخدم في بطاقة المشرف."""
    try:
        rows = _select("students", {"id": int(record_id)}, limit=1)
    except Exception:
        return None
    return rows[0] if rows else None


def find_students(query, limit=12):
    """يبحث عن طالب بالاسم أو الرقم الجامعي أو المعرّف (بحث جزئي متسامح)."""
    q = " ".join((query or "").split()).lower()
    if not q:
        return []
    out = []
    for s in list_students(limit=5000):
        haystack = " ".join([
            str(s.get("full_name") or ""),
            str(s.get("uni_id") or ""),
            str(s.get("user_id") or ""),
            str(s.get("username") or ""),
        ]).lower()
        if q in haystack:
            out.append(s)
        if len(out) >= limit:
            break
    return out


def delete_student(record_id):
    """يحذف سجل طالب واحد نهائيًا — للمشرف فقط."""
    _delete("students", {"id": int(record_id)})
    _cache_clear()
    with _DIAG_LOCK:
        DIAG["students"] = count_students()


def delete_all_students():
    """يحذف كل سجلات الطلاب — للمشرف فقط (يُطلب تأكيد صريح قبله)."""
    before = count_students()
    _delete_all("students")
    _cache_clear()
    with _DIAG_LOCK:
        DIAG["students"] = count_students()
    return before


def all_chat_ids():
    """معرّفات المحادثات لإرسال البث — بلا تكرار ومرتّبة."""
    ids, seen = [], set()
    for s in list_students():
        cid = str(s.get("chat_id") or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            ids.append(cid)
    return ids


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
        "settings": {r["key"]: r["value"] for r in _select("settings")
                     if not str(r["key"]).startswith("_pending_")},
        "year_groups": {str(r["year"]): r["group_link"]
                        for r in _select("year_groups")},
        "specializations": _select("specializations",
                                   order=[("position", "asc"), ("id", "asc")]),
        "courses": _select("courses", order=[("position", "asc"), ("id", "asc")]),
        "students": _select("students", order=[("id", "asc")]),
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
            "theory": c.get("theory", ""),
            "labs": c.get("labs", ""),
            "past": c.get("past", ""),
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
BOT_TOKEN = (os.getenv("BOT_TOKEN", "")
             or os.getenv("TELEGRAM_BOT_TOKEN", "")
             or os.getenv("TELEGRAM_TOKEN", "")
             or "").strip().strip('"').strip("'").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

# كشف عنوان الاستضافة تلقائيًا (Render / Railway / Koyeb) ليعمل الويب هوك بلا إعداد يدوي
if not WEBHOOK_URL:
    for _var in ("RENDER_EXTERNAL_URL", "RAILWAY_PUBLIC_DOMAIN", "KOYEB_PUBLIC_DOMAIN"):
        _val = os.getenv(_var, "").strip()
        if _val:
            WEBHOOK_URL = _val if _val.startswith("http") else "https://" + _val
            break
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "uniguide-secret") or "uniguide-secret"
# تلغرام يسمح فقط بحروف/أرقام و _ و - في التوقيع؛ نُنظّفه حتى لا يفشل التسجيل
import re as _re
WEBHOOK_SECRET = _re.sub(r"[^A-Za-z0-9_-]", "-", WEBHOOK_SECRET).strip("-") or "uniguide-secret"
WEBHOOK_SECRET = WEBHOOK_SECRET[:256]
WEBHOOK_PATH = (os.getenv("WEBHOOK_PATH", "/telegram") or "/telegram").strip()
if WEBHOOK_PATH and not WEBHOOK_PATH.startswith("/"):
    WEBHOOK_PATH = "/" + WEBHOOK_PATH
if not WEBHOOK_PATH:
    WEBHOOK_PATH = "/telegram"
PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
# هل نطلب من الطالب تسجيل بياناته (الاسم + الرقم الجامعي) عند أول استخدام؟
ASK_STUDENT_DATA = (os.getenv("ASK_STUDENT_DATA", "1").strip().lower()
                    not in ("0", "false", "no", "off"))
# معرّفات محادثات المشرفين (اختيارية): تُستخدم للإشعارات
ADMIN_CHAT_IDS = {x.strip() for x in os.getenv("ADMIN_CHAT_IDS", "").split(",") if x.strip()}
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
# حقول مظهر البوت وإعداداته (يعدّلها المشرف من لوحة التحكم)
BOT_FIELDS = [
    ("bot_title", "عنوان القائمة الرئيسية"),
    ("bot_subtitle", "سطر الترحيب"),
    ("footer_text", "نص التذييل أسفل الرسائل"),
    ("contact_link", "رابط التواصل / الدعم"),
    ("channel_link", "رابط القناة الرسمية"),
]

# ---- الملفات المرفوعة مباشرة إلى تلغرام -------------------------------------
# عندما يرفع المشرف ملفًا (PDF/Word/صورة…) نخزّن «معرّف الملف» الذي يمنحه
# تلغرام، ثم نعيد إرسال الملف نفسه للطالب لاحقًا. لا نحتاج تنزيله ولا رفعه
# إلى أي خدمة تخزين، وهذا يتجاوز حدود الحجم تمامًا.
TG_FILE_PREFIX = "tgfile:"

# نوع الملف: صورة تُرسَل بـ sendPhoto، وما عداها بـ sendDocument.
MEDIA_PHOTO = "photo"
MEDIA_DOC = "doc"


def pack_tg_file(file_id, name="", media=MEDIA_DOC):
    """يخزّن معرّف الملف واسمه ونوعه في قيمة واحدة قابلة للحفظ في القاعدة.

    الصيغة: tgfile:<kind>|<file_id>|<name>"""
    kind = MEDIA_PHOTO if str(media or "").lower() == MEDIA_PHOTO else MEDIA_DOC
    return (TG_FILE_PREFIX + kind + "|" + str(file_id) + "|"
            + (name or "ملف").replace("|", " "))


def unpack_tg_file(value):
    """يفكّ القيمة ويعيد (file_id, name, media) إن كان ملفًا مرفوعًا، وإلا None."""
    v = str(value or "")
    if not v.startswith(TG_FILE_PREFIX):
        return None
    body = v[len(TG_FILE_PREFIX):]
    media = MEDIA_DOC
    if body.startswith(MEDIA_PHOTO + "|"):
        media = MEDIA_PHOTO
        body = body[len(MEDIA_PHOTO) + 1:]
    elif body.startswith(MEDIA_DOC + "|"):
        body = body[len(MEDIA_DOC) + 1:]
    if "|" in body:
        fid, name = body.split("|", 1)
    else:
        fid, name = body, "ملف"
    fid = fid.strip()
    if not fid:
        return None
    return fid, (name.strip() or "ملف"), media


def is_tg_file(value):
    return unpack_tg_file(value) is not None


def send_tg_file(chat_id, file_id, caption="", keyboard=None, media=MEDIA_DOC):
    """يعيد إرسال ملف مرفوع مسبقًا للطالب/المشرف — بلا تنزيل ولا رفع.

    الصور تُرسَل بـ sendPhoto كي تظهر داخل المحادثة، وإن فشل ذلك نتراجع
    إلى sendDocument (بعض معرّفات الصور القديمة لا تعمل مع sendPhoto)."""
    def _call(method, field):
        kwargs = {"chat_id": chat_id, field: file_id}
        if caption:
            kwargs["caption"] = caption
            kwargs["parse_mode"] = "HTML"
        if keyboard:
            kwargs["reply_markup"] = keyboard
        return api(method, **kwargs)

    if media == MEDIA_PHOTO:
        res = _call("sendPhoto", "photo")
        if isinstance(res, dict) and res.get("ok") is False:
            res = _call("sendDocument", "document")
        return res
    return _call("sendDocument", "document")


def _deliver_group_file(chat_id, label, value, keyboard=None):
    """يُسلّم ملف/رابط مجموعة السنة أو التخصص للطالب."""
    packed = unpack_tg_file(value)
    if packed:
        fid, fname, media = packed
        res = send_tg_file(chat_id, fid,
                           caption=f"👥 <b>{esc(label)}</b>\n📎 {esc(fname)}\n\n"
                                   "<i>احفظ الملف عندك للرجوع إليه لاحقًا.</i>",
                           keyboard=keyboard, media=media)
        if isinstance(res, dict) and res.get("ok") is False:
            send(chat_id, "⚠️ تعذّر إرسال الملف — غالبًا حُذف من خدمة تلغرام.\n"
                          "أبلغ المشرف ليعيد رفعه.", keyboard)
        return
    if value:
        send(chat_id, f"👥 <a href='{esc(value)}'>{esc(label)}</a>", keyboard)
    else:
        send(chat_id, "⚠️ غير متوفر حاليًا.", keyboard)


COURSE_FIELDS = [
    ("theory", "📖 محاضرات نظرية"),
    ("labs", "💻 مخابر عملي"),
    ("past", "📝 دورات سابقة"),
    ("lectures", "🎧 تسجيلات المحاضرات"),
    ("practical", "🧪 مواد العملي"),
    ("courses", "🗂 دورات ومراجع"),
    ("files", "📎 ملفات المقرر"),
]
# التصنيفات الأساسية الثلاثة التي يراها الطالب إن وُجدت
COURSE_MAIN_FIELDS = ("theory", "labs", "past")

# الذاكرة المؤقتة: من ينتظر إدخال قيمة، وماذا ينتظر بالضبط
PENDING = {}
ADMIN_STATE = set()

# نحفظ «سياق التعديل» مؤقتًا في قاعدة البيانات كذلك، حتى لا يضيع
# إذا أُعيد تشغيل الخدمة بين ضغط المشرف للحقل ورفعه للملف.
_PENDING_TTL = 6 * 3600  # ست ساعات


def save_pending(uid, action):
    """يحفظ السياق في الذاكرة وفي قاعدة البيانات معًا (حماية من إعادة التشغيل)."""
    PENDING[uid] = action
    try:
        _upsert("settings", {"key": "_pending_" + str(uid),
                             "value": json.dumps(action, ensure_ascii=False)},
                ["key"])
        _cache_drop(("all_settings",))
    except Exception as exc:  # لا نُفشل العملية بسبب الحفظ الاحتياطي
        print("[save_pending warn]", exc)


def load_pending(uid):
    """يستعيد السياق (من الذاكرة أولًا، ثم من قاعدة البيانات)."""
    if uid in PENDING:
        return PENDING[uid]
    try:
        raw = dict((r["key"], r["value"]) for r in _select("settings")).get(
            "_pending_" + str(uid))
    except Exception:
        raw = None
    if not raw:
        return None
    try:
        action = json.loads(raw)
    except Exception:
        return None
    if isinstance(action, list):
        action = tuple(action)
        if len(action) == 2 and isinstance(action[1], list):
            action = (action[0], tuple(action[1]))
    PENDING[uid] = action
    return action


def clear_pending(uid):
    """يمسح السياق من الذاكرة ومن قاعدة البيانات."""
    PENDING.pop(uid, None)
    try:
        _delete("settings", {"key": "_pending_" + str(uid)})
        _cache_drop(("all_settings",))
    except Exception:
        pass

# ------------------------------------------------------------------ تشخيص
DIAG = {
    "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    "mode": "غير محدّد",
    "received_updates": 0,
    "last_update_id": None,
    "last_received_at": None,
    "last_reply_ok": None,
    "last_reply_error": None,
    "token_ok": None,
    "bot_username": None,
    "token_message": None,
    "webhook_url": None,
    "pending_count": None,
    "last_webhook_error": None,
    "storage_ok": None,
    "storage_note": None,
    "last_db_error": None,
    "students": 0,
    "broadcast_sent": 0,
}
_DIAG_LOCK = threading.Lock()


# ---------------------------------------------------------------- telegram API
def api(method, **params):
    """نداء واحد إلى واجهة تلغرام، عبر الاتصال المُعاد استخدامه إن توفّر."""
    url = API.format(token=BOT_TOKEN, method=method)
    tries = int(params.pop("_tries", 3))
    # نتجاهل أي قيمة فارغة (None) حتى لا يرفضها تلغرام
    params = {k: v for k, v in params.items() if v is not None}
    data = json.dumps(params, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    last = None
    for attempt in range(max(1, tries)):
        try:
            code, raw = _http_perform("POST", url, headers=headers, content=data,
                                      timeout=70)
            try:
                result = json.loads(raw) if raw.strip() else {"ok": False}
            except Exception:
                result = {"ok": False, "description": raw[:200]}
            if 200 <= code < 300:
                if method in ("sendMessage", "editMessageText"):
                    with _DIAG_LOCK:
                        DIAG["last_reply_ok"] = bool(result.get("ok"))
                        DIAG["last_reply_error"] = None if result.get("ok") else str(
                            result.get("description", "غير معروف"))
                        if not result.get("ok"):
                            print("⚠️ فشل إرسال الرد:", result.get("description"))
                return result
            last = "HTTP %s %s" % (code, result.get("description", ""))
        except Exception as exc:  # pragma: no cover
            last = f"{type(exc).__name__}: {exc}"
        if attempt < max(1, tries) - 1:
            time.sleep(1)
    print("[api error]", method, last)
    if method in ("sendMessage", "editMessageText"):
        with _DIAG_LOCK:
            DIAG["last_reply_ok"] = False
            DIAG["last_reply_error"] = str(last)
    return {"ok": False, "description": last}


def _plain(text):
    """يزيل وسوم HTML للاستخدام في المحاولة الاحتياطية."""
    return re.sub(r"<[^>]+>", "", str(text))


def send(chat_id, text, keyboard=None, preview=False):
    """يرسل رسالة، وإن رفض تلغرام الصيغة يعيد المحاولة بصيغة أبسط
    حتى لا يبقى أي رد بلا إجابة مهما كان السبب."""
    # الأزرار يجب أن تكون كائناً (dict) لا نصاً
    if isinstance(keyboard, str):
        try:
            keyboard = json.loads(keyboard)
        except Exception:
            keyboard = None
    if keyboard is not None and not isinstance(keyboard, dict):
        keyboard = None

    # محاولة ①: كاملة (HTML + الأزرار)
    attempts = []
    if keyboard:
        attempts.append({"parse_mode": "HTML", "reply_markup": keyboard})
    attempts.append({"parse_mode": "HTML"})
    # محاولة أخيرة: نص صافٍ بلا أي تنسيق
    attempts.append({"__plain__": True})

    result = {"ok": False, "description": "لم تُجرَّب أي محاولة"}
    for idx, extra in enumerate(attempts):
        kwargs = {
            "chat_id": chat_id,
            "text": _plain(text) if extra.get("__plain__") else text,
            "disable_web_page_preview": not preview,
            "_tries": 1,
        }
        if extra.get("parse_mode"):
            kwargs["parse_mode"] = extra["parse_mode"]
        if extra.get("reply_markup"):
            kwargs["reply_markup"] = extra["reply_markup"]
        result = api("sendMessage", **kwargs)
        if result.get("ok"):
            if idx > 0:
                print(f"⚠️ تم الإرسال بصيغة احتياطية رقم {idx + 1}")
            return result
    return result


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


def send_bytes_file(chat_id, filename, blob, caption="", mime="text/csv"):
    """يرسل أي ملف (نص/بايتات) كوثيقة تلغرام — للتقارير وملفات Excel."""
    if isinstance(blob, str):
        blob = blob.encode("utf-8-sig")
    boundary = "----uniguide" + str(int(time.time() * 1000))
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
        f'filename="{filename}"\r\nContent-Type: {mime}\r\n\r\n'.encode("utf-8"))
    parts.append(blob if isinstance(blob, bytes) else blob.encode("utf-8"))
    parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    url = API.format(token=BOT_TOKEN, method="sendDocument")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover
        print("[sendDocument error]", exc)
        return {"ok": False}


def _xlsx_escape(value):
    text = "" if value is None else str(value)
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace('"', "&quot;"))


def make_xlsx(sheet_name, header, rows):
    """يبني ملف Excel (.xlsx) حقيقيًا بلا أي مكتبات خارجية.

    نكتب ملفات XML الأساسية داخل حاوية ZIP — وهو ما يقرأه Excel مباشرة."""
    import zipfile

    def col_letter(n):
        s = ""
        n += 1
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def sheet_xml():
        out = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
               '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
               "<sheetData>"]
        all_rows = [header] + rows
        for ri, row in enumerate(all_rows, 1):
            out.append('<row r="%d">' % ri)
            for ci, val in enumerate(row):
                ref = "%s%d" % (col_letter(ci), ri)
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    out.append('<c r="%s"><v>%s</v></c>' % (ref, val))
                else:
                    out.append('<c r="%s" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>'
                               % (ref, _xlsx_escape(val)))
            out.append("</row>")
        out.append("</sheetData></worksheet>")
        return "".join(out)

    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>")
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>")
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="%s" sheetId="1" r:id="rId1"/></sheets></workbook>'
        % _xlsx_escape(sheet_name[:28] or "Sheet1"))
    wb_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        "</Relationships>")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", root_rels)
        z.writestr("xl/workbook.xml", workbook)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        z.writestr("xl/worksheets/sheet1.xml", sheet_xml())
    return buf.getvalue()


def csv_text(header, rows):
    """نص CSV بترميز يفهمه Excel العربي (BOM داخليًا)."""
    import csv

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([str(h) for h in header])
    for r in rows:
        w.writerow(["" if v is None else str(v) for v in r])
    return buf.getvalue()


def notify_admins(text, keyboard=None):
    """إشعار فوري لكل المشرفين المعروفين."""
    targets = set(DEFAULT_ADMIN_IDS) | set(ADMIN_STATE)
    for s in db.list_students(limit=500):
        if str(s.get("chat_id") or "") in ADMIN_CHAT_IDS:
            targets.add(int(s["chat_id"]) if str(s["chat_id"]).isdigit() else s["chat_id"])
    for cid in targets:
        try:
            send(cid, text, keyboard)
        except Exception as exc:  # pragma: no cover
            print("[notify warning]", exc)


def download_file(file_id):
    """ينزّل ملفًا من تلغرام ويرجع محتواه بايتات."""
    info = api("getFile", file_id=file_id)
    if not info.get("ok"):
        return None
    path = info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
    try:
        code, body = _http_perform_raw("GET", url, timeout=70)
        if 200 <= code < 300:
            return body
        print("[getFile error] HTTP", code)
        return None
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


def link_line(value):
    """يعرض رابطًا كزر نصّي آمن (أو النص كما هو إن لم يكن رابطًا)."""
    v = esc(value)
    if re.match(r"^(https?://|tg://)", str(value).strip()):
        return "<a href='" + str(value).strip() + "'>" + v + "</a>"
    return v


def group_link_block(label, value, owner_cb):
    """يبني سطر «رابط المجموعة» مع زر تنزيل إن كان الملف مرفوعًا.

    يعيد (نص السطر, زر التنزيل أو None)."""
    packed = unpack_tg_file(value)
    if packed:
        return (f"👥 <b>{esc(label)}:</b> 📎 {esc(packed[1])}",
                btn("📥 عرض " + label, owner_cb))
    if value:
        return (f"👥 <a href='{esc(value)}'>{esc(label)}</a>", None)
    return (f"👥 {esc(label)}: غير متوفر", None)


def links_block(pairs):
    """يبني سطور الروابط/الملفات المتوفرة فقط."""
    lines = []
    for label, value in pairs:
        if not value:
            continue
        packed = unpack_tg_file(value)
        if packed:
            lines.append(f"▫️ {label}: 📎 {esc(packed[1])}")
        else:
            lines.append(f"▫️ {label}: <a href='{esc(value)}'>{esc(value)}</a>")
    return "\n".join(lines)


# --------------------------------------------------------------------- screens
DIVIDER = "━━━━━━━━━━━━━━━"


# ------------------------------------------------------------------ breadcrumb
HOME_CRUMB = "📍 الرئيسية"


def crumbs(*parts):
    """شريط مسار بصري: 📍 الرئيسية › السنة الثالثة › تخصص المحاسبة

    يوضح للطالب مكانه الحالي في أي شاشة عميقة."""
    items = [HOME_CRUMB] + [str(p).strip() for p in parts if str(p or "").strip()]
    return "🧭 <i>" + " › ".join(items) + "</i>"


def home_button(label="🏠 القائمة الرئيسية", data="menu"):
    return btn(label, data)


def go_home_row(label="🏠 القائمة الرئيسية"):
    """صف ثابت يظهر آخر كل قائمة عميقة: عودة بضغطة واحدة للرئيسية."""
    return [home_button(label)]


def grid(buttons, per_row=2):
    """يرتّب الأزرار في شبكة أنيقة (2×2 افتراضيًا) مريحة من الهاتف."""
    return [list(buttons[i:i + per_row]) for i in range(0, len(buttons), per_row)]


def main_menu(uid=None):
    """القائمة الرئيسية — أزرار مرتّبة في شبكة 2×2 مريحة من الهاتف."""
    title = db.get_setting("bot_title") or db.get_setting("university_name") or "المرشد الجامعي"
    subtitle = db.get_setting("bot_subtitle") or "مرحبًا بك! اختر القسم الذي تريده 👇"
    student = db.get_student(uid) if uid else None

    parts = ["🎓 <b>" + esc(title) + "</b>", DIVIDER]
    if student and student.get("full_name"):
        parts.append("👋 أهلًا " + esc(student["full_name"]) +
                     (" • " + esc(student.get("uni_id")) if student.get("uni_id") else ""))
    parts.append(esc(subtitle))
    text = "\n".join(parts)
    footer = db.get_setting("footer_text")
    if footer:
        text += "\n\n" + esc(footer)

    # (1) شبكة 2×2 للأقسام الأساسية — كل صف زرّان متساويان
    grid_buttons = [
        btn("📢 المعلومات العامة", "gen"),
        btn("📅 الجداول الدراسية", "sched"),
        btn("📘 السنة الأولى", "y:1"),
        btn("📗 السنة الثانية", "y:2"),
        btn("📙 السنة الثالثة", "y:3"),
        btn("📕 السنة الرابعة", "y:4"),
    ]
    rows = grid(grid_buttons, 2)
    # (2) أدوات: البحث والإعدادات جنبًا إلى جنب
    rows.append([btn("🔎 بحث عن مقرر", "search"), btn("⚙️ إعداداتي", "me")])
    # (3) التواصل (يظهران فقط إن أضاف المشرف روابطهما)
    extra = []
    if db.get_setting("contact_link"):
        extra.append(btn("📞 تواصل معنا", "contact"))
    if db.get_setting("channel_link"):
        extra.append(btn("📣 القناة الرسمية", "channel"))
    if extra:
        rows.append(extra)
    return text, {"inline_keyboard": rows}


def contact_screen():
    """شاشة التواصل والقناة."""
    lines = [crumbs("تواصل معنا"), "📞 <b>تواصل معنا</b>", DIVIDER, ""]
    contact = db.get_setting("contact_link")
    channel = db.get_setting("channel_link")
    found = False
    if contact:
        lines.append("• الدعم والاستفسار: <a href='" + esc(contact) + "'>اضغط هنا</a>")
        found = True
    if channel:
        lines.append("• القناة الرسمية: <a href='" + esc(channel) + "'>اضغط هنا</a>")
        found = True
    if not found:
        lines.append("⚠️ لم تُضَف أي وسيلة تواصل بعد.")
    rows = [[btn("📣 القناة الرسمية", "channel")], go_home_row()]
    return "\n".join(lines), {"inline_keyboard": rows}


def channel_screen():
    lines = [crumbs("القناة الرسمية"), "📣 <b>القناة الرسمية</b>", DIVIDER, ""]
    channel = db.get_setting("channel_link")
    if channel:
        lines.append("<a href='" + esc(channel) + "'>اضغط للانضمام إلى القناة</a>")
    else:
        lines.append("⚠️ لم يُضَف رابط القناة بعد.")
    return "\n".join(lines), {"inline_keyboard": [go_home_row()]}


def general_screen():
    lines = [crumbs("المعلومات العامة"), "📢 <b>المعلومات العامة والتعاميم</b>", DIVIDER, ""]
    body = links_block([(label, db.get_setting(key)) for key, label in GENERAL_FIELDS])
    lines.append(body if body else "⚠️ لم تُضَف أي معلومات بعد.")
    return "\n".join(lines), {"inline_keyboard": [go_home_row()]}


def schedule_screen():
    """الجداول الدراسية والامتحانية: محاضرات، قاعات، امتحانات."""
    lines = [crumbs("الجداول الدراسية والامتحانية"),
             "📅 <b>الجداول الدراسية والامتحانية</b>", DIVIDER, "",
             "<i>كل الجداول الرسمية في مكان واحد:</i>", ""]
    body = links_block([(label, db.get_setting(key)) for key, label in SCHEDULE_FIELDS])
    lines.append(body if body else "⚠️ لم تُضَف أي جداول بعد.")
    lines.append("")
    lines.append("💡 <i>لمتابعة أي تحديث على الجداول، انضم إلى قناة الكلية أو قناة سنتك.</i>")
    return "\n".join(lines), {"inline_keyboard": [go_home_row()]}


def year_screen(year):
    """شاشة السنة: مقرراتها (أو تخصصاتها في السنة الثالثة) + عودة سريعة."""
    lines = [crumbs(YEAR_TITLES[year]), "<b>" + YEAR_TITLES[year] + "</b>", DIVIDER, ""]
    gl_line, gl_btn = group_link_block("رابط مجموعة السنة", db.get_year_group(year),
                                       f"gl:y:{year}")
    lines.append(gl_line)
    lines.append("")

    if year == 3:
        specs = db.list_specs()
        if not specs:
            lines.append("⚠️ لم تُضَف تخصصات بعد.")
            return "\n".join(lines), {"inline_keyboard": [go_home_row()]}
        lines.append("🎯 <b>التخصصات المتاحة:</b>")
        lines.append("<i>اضغط على تخصصك ليظهر لك مقرراته فقط.</i>")
        # كل تخصص في سطر مستقل — لا تُخلط المقررات هنا أبدًا
        rows = []
        if gl_btn:
            rows.append([gl_btn])
        rows += [[btn(f"🎯 {s['name']}", f"sp:{s['id']}")] for s in specs]
        rows.append(go_home_row())
        return "\n".join(lines), {"inline_keyboard": rows}

    courses = db.list_courses(year)
    if not courses:
        lines.append("⚠️ لم تُضَف مقررات بعد.")
    else:
        lines.append("📚 <b>المقررات:</b>")
        for i, c in enumerate(courses, 1):
            lines.append(f"{i}. {esc(c['name'])}")
    rows = []
    if gl_btn:
        rows.append([gl_btn])
    rows += [[btn(f"📖 {c['name']}", f"c:{c['id']}")] for c in courses]
    rows.append(go_home_row())
    return "\n".join(lines), {"inline_keyboard": rows}


def spec_screen(spec_id):
    spec = db.get_spec(spec_id)
    if not spec:
        return "⚠️ التخصص غير موجود.", {"inline_keyboard": [go_home_row()]}
    lines = [crumbs(YEAR_TITLES[3], "تخصص: " + spec["name"]),
             "🎯 <b>" + esc(spec["name"]) + "</b>", DIVIDER, ""]
    gl_line, gl_btn = group_link_block("رابط مجموعة التخصص", spec["group_link"],
                                       f"gl:sp:{spec_id}")
    lines.append(gl_line)
    lines.append("")
    lines.append("📚 <b>مقررات تخصص «" + esc(spec["name"]) + "»:</b>")
    courses = db.list_courses(3, spec_id)   # مقررات هذا التخصص فقط
    if not courses:
        lines.append("<i>لم تُضَف مقررات لهذا التخصص بعد.</i>")
    else:
        for i, c in enumerate(courses, 1):
            lines.append(f"{i}. {esc(c['name'])}")
    rows = []
    if gl_btn:
        rows.append([gl_btn])
    rows += [[btn(f"📖 {c['name']}", f"c:{c['id']}")] for c in courses]
    rows.append(go_home_row())
    return "\n".join(lines), {"inline_keyboard": rows}


def course_screen(course_id):
    """شاشة المقرر بتصنيفات أدق: نظري / عملي / دورات سابقة / ملفات."""
    c = db.get_course(course_id)
    if not c:
        return "⚠️ المقرر غير موجود.", {"inline_keyboard": [go_home_row()]}
    spec_name = None
    if c.get("spec_id"):
        spec = db.get_spec(c["spec_id"])
        spec_name = spec["name"] if spec else None
    path = [YEAR_TITLES.get(c["year"], "-")]
    if spec_name:
        path.append("تخصص: " + spec_name)
    lines = [crumbs(*path), "📖 <b>" + esc(c["name"]) + "</b>", DIVIDER]
    if spec_name:
        lines.append(f"🎯 التخصص: {esc(spec_name)}")
    lines.append("")

    # التصنيفات الأساسية الثلاثة: تظهر بأيقونة واضحة
    lines.append("<b>اختر ما تريد من هذا المقرر:</b>")
    found_any = False
    for key, label in COURSE_FIELDS:
        val = (c.get(key) or "").strip()
        if not val:
            continue
        found_any = True
        packed = unpack_tg_file(val)
        if packed:
            lines.append(f"▫️ {esc(label)}: 📎 {esc(packed[1])}")
        else:
            lines.append(f"▫️ {esc(label)}: <a href='{esc(val)}'>اضغط هنا</a>")
    if not found_any:
        lines.append("<i>⚠️ لم تُضَف ملفات لهذا المقرر بعد.</i>")

    rows = []
    for key, label in COURSE_FIELDS:
        val = (c.get(key) or "").strip()
        if val:
            mark = "📎" if unpack_tg_file(val) else "🔗"
            rows.append([btn(mark + " " + label, f"f:{course_id}:{key}")])
    rows.append([btn("⬅️ رجوع إلى المقررات", f"y:{c['year']}" if not c.get("spec_id") else f"sp:{c['spec_id']}")])
    rows.append(go_home_row())
    return "\n".join(lines), {"inline_keyboard": rows}


def _deliver_course_file(chat_id, course_id, field, back_label="⬅️ رجوع إلى المقرر"):
    """يُسلّم ملف المقرر للطالب: ملف مرفوع مباشرةً، أو رابط يُفتح.

    هذه الدالة ترسل الملف نفسه في المحادثة عندما يكون مرفوعًا إلى تلغرام،
    أو تعرض زرًا للرابط إن كان المشرف قد وضع رابطًا خارجيًا."""
    c = db.get_course(course_id)
    label = dict(COURSE_FIELDS).get(field, "الملف")
    if not c or not (c.get(field) or "").strip():
        send(chat_id, "⚠️ هذا الملف غير متوفر حاليًا.",
             {"inline_keyboard": [[btn(back_label, f"c:{course_id}")], go_home_row()]})
        return
    value = str(c.get(field) or "").strip()
    caption = ("🔗 <b>" + label + "</b>\n" + DIVIDER + "\n📖 المقرر: <b>"
               + esc(c["name"]) + "</b>")
    packed = unpack_tg_file(value)
    if packed:
        fid, fname, media = packed
        res = send_tg_file(chat_id, fid,
                           caption=caption + "\n\n<i>احفظ الملف عندك للرجوع إليه لاحقًا.</i>",
                           keyboard={"inline_keyboard": [[btn(back_label, f"c:{course_id}")],
                                                         go_home_row()]},
                           media=media)
        if not (isinstance(res, dict) and res.get("ok") is False):
            return
        # إن فشل الإرسال (ملف قديم محذوف من تلغرام) نُعلم بوضوح بدل الصمت
        send(chat_id,
             "⚠️ تعذّر إرسال الملف المرفوع — غالبًا حُذف من خدمة تلغرام.\n"
             "أبلغ المشرف ليعيد رفع الملف.",
             {"inline_keyboard": [[btn(back_label, f"c:{course_id}")], go_home_row()]})
        return
    # رابط خارجي عادي
    lines = [crumbs(YEAR_TITLES.get(c["year"], "-")), "🔗 <b>" + esc(label) + "</b>",
             DIVIDER, "", "📖 المقرر: <b>" + esc(c["name"]) + "</b>", "",
             "<a href='" + esc(value) + "'>اضغط هنا لفتح الملف</a>"]
    rows = [[btn(back_label, f"c:{course_id}")], go_home_row()]
    send(chat_id, "\n".join(lines), {"inline_keyboard": rows})


def course_file_screen(course_id, field):
    """يعرض تفاصيل ملف المقرر مع إمكانية تسليمه مباشرة."""
    c = db.get_course(course_id)
    label = dict(COURSE_FIELDS).get(field, "الملف")
    if not c or not (c.get(field) or "").strip():
        return "⚠️ هذا الملف غير متوفر حاليًا.", {"inline_keyboard": [go_home_row()]}
    value = str(c.get(field) or "").strip()
    packed = unpack_tg_file(value)
    path = [YEAR_TITLES.get(c["year"], "-")]
    if c.get("spec_id"):
        spec = db.get_spec(c["spec_id"])
        if spec:
            path.append("تخصص: " + spec["name"])
    path.append(c["name"])
    lines = [crumbs(*path), "🔗 <b>" + esc(label) + "</b>", DIVIDER, "",
             "📖 المقرر: <b>" + esc(c["name"]) + "</b>", ""]
    if packed:
        fid, fname, _media = packed
        lines.append("📎 الملف: <b>" + esc(fname) + "</b>")
        lines.append("<i>اضغط الزر لعرض الملف داخل المحادثة.</i>")
        rows = [[btn("📥 عرض الملف", f"df:{course_id}:{field}")]]
    else:
        lines.append("🔗 <a href='" + esc(value) + "'>اضغط هنا لفتح الملف</a>")
        rows = []
    rows.append([btn("⬅️ رجوع إلى المقرر", f"c:{course_id}")])
    rows.append(go_home_row())
    return "\n".join(lines), {"inline_keyboard": rows}


# ------------------------------------------------------- إعدادات الطالب
def me_screen(uid):
    """ملف الطالب الشخصي: تعديل الاسم والرقم الجامعي بنفسه."""
    s = db.get_student(uid) or {}
    lines = [crumbs("إعداداتي"), "⚙️ <b>إعداداتي</b>", DIVIDER, ""]
    lines.append("• الاسم: <b>" + (esc(s.get("full_name")) or "— لم يُسجَّل —") + "</b>")
    lines.append("• الرقم الجامعي: <b>" + (esc(s.get("uni_id")) or "— لم يُسجَّل —") + "</b>")
    if s.get("year"):
        lines.append("• السنة: <b>" + esc(YEAR_TITLES.get(s.get("year"), s.get("year"))) + "</b>")
    lines.append("")
    lines.append("<i>أي تعديل تحفظه هنا يصل إشعار فوري لإدارة الكلية ✅</i>")
    rows = []
    if not (s.get("full_name") and s.get("uni_id")):
        rows.append([btn("📝 إكمال بياناتي (الاسم + الرقم)", "me:reg")])
    rows += [
        [btn("✏️ تعديل الاسم", "me:name"), btn("🔢 تعديل الرقم الجامعي", "me:uni")],
        [btn("🎓 تعديل السنة", "me:year")],
        go_home_row(),
    ]
    return "\n".join(lines), {"inline_keyboard": rows}


def me_year_screen(uid):
    s = db.get_student(uid) or {}
    lines = [crumbs("إعداداتي", "تعديل السنة"), "🎓 <b>اختر سنتك الدراسية</b>", DIVIDER, ""]
    rows = [[btn(YEAR_TITLES[y], f"me:setyear:{y}")] for y in YEARS]
    rows.append([btn("⬅️ رجوع", "me")])
    rows.append(go_home_row())
    return "\n".join(lines), {"inline_keyboard": rows}


def valid_uni_id(value):
    """تحقق الرقم الجامعي: أرقام فقط، من 4 إلى 20 خانة (يمنع الإدخال العشوائي)."""
    v = (value or "").strip()
    if not re.fullmatch(r"[0-9]{4,20}", v):
        return False, ("الرقم الجامعي يجب أن يكون <b>أرقامًا فقط</b> "
                       "وطوله من 4 إلى 20 رقمًا.")
    return True, v


def valid_name(value):
    """تحقق الاسم: حروف عربية/إنجليزية ومسافات فقط، بلا أرقام أو رموز."""
    v = " ".join((value or "").split())          # تنظيف المسافات الزائدة
    if len(v) < 2:
        return False, "الاسم قصير جدًا، اكتب اسمك كما في السجل الجامعي."
    if len(v) > 64:
        return False, "الاسم طويل جدًا، اكتب اسمك الثلاثي فقط."
    # لا أرقام ولا رموز: حروف عربية أو إنجليزية (مع الهمزات والتشديد) والمسافة
    if not re.fullmatch(r"[\u0621-\u064A\u0660-\u0669a-zA-Z \u0629\u0649\u0640\u0671-\u06D3'\-]+", v):
        return False, "اكتب الاسم بحروف فقط (بلا أرقام أو رموز)."
    if re.search(r"[0-9\u0660-\u0669]", v):
        return False, "الاسم لا يحتوي أرقامًا — الأرقام للرقم الجامعي فقط."
    if not re.search(r"[a-zA-Z\u0621-\u064A]", v):
        return False, "اكتب اسمك بحروف واضحة."
    if len(re.sub(r"[^a-zA-Z\u0621-\u064A]", "", v)) < 2:
        return False, "الاسم قصير جدًا، اكتب اسمك كما في السجل الجامعي."
    return True, v[:64]


# ---------------------------------------------------------------- admin panel
def admin_menu():
    text = ("🛠 <b>لوحة التحكم</b>\n" + DIVIDER
            + "\nاختر ما تريد إدارته:")
    kb = {
        "inline_keyboard": [
            # المحتوى (شبكة 2×2)
            [btn("📢 المعلومات العامة", "a:gen"), btn("📅 الجداول", "a:sched")],
            [btn("📘 الأولى", "a:y:1"), btn("📗 الثانية", "a:y:2")],
            [btn("📙 الثالثة", "a:y:3"), btn("📕 الرابعة", "a:y:4")],
            # التخصصات والطلاب
            [btn("🎯 إدارة التخصصات", "a:specs"), btn("👥 الطلاب", "a:students")],
            # التعاميم والتقارير
            [btn("📣 بث عام", "a:bc"), btn("📤 تصدير البيانات", "a:export")],
            # المظهر والنسخ
            [btn("🎨 مظهر البوت واسمه", "a:bot")],
            [btn("💾 نسخة احتياطية", "a:backup"), btn("📥 استعادة نسخة", "a:restore")],
            # الأدوات
            [btn("🔍 فحص التخزين", "a:testdb"), btn("🩺 حالة البوت", "a:status")],
            [btn("🏠 القائمة الرئيسية", "menu")],
        ]
    }
    return text, kb


STUDENTS_PER_PAGE = 8


def _student_row_label(s):
    """سطر واحد يصف الطالب: الاسم — الرقم الجامعي."""
    nm = (s.get("full_name") or "").strip() or "بدون اسم"
    un = (s.get("uni_id") or "").strip()
    label = nm + (" — " + un if un else " — بلا رقم")
    return label[:48]


def admin_students_screen(page=0):
    """قائمة الطلاب مع بحث وترقيم صفحات — ومنها الحذف."""
    students = db.list_students(limit=5000)
    total = len(students)
    total_pages = max(1, (total + STUDENTS_PER_PAGE - 1) // STUDENTS_PER_PAGE)
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * STUDENTS_PER_PAGE
    chunk = students[start:start + STUDENTS_PER_PAGE]

    lines = [crumbs("لوحة التحكم", "الطلاب"), "👥 <b>إدارة الطلاب</b>", DIVIDER, ""]
    lines.append("العدد الكلي: <b>" + str(total) + "</b>"
                 + ("   •   صفحة " + str(page + 1) + " من " + str(total_pages) if total else ""))
    lines.append("")
    if not students:
        lines.append("لا يوجد طلاب مسجّلون بعد.")
        lines.append("<i>سيُضاف الطلاب تلقائيًا عند استخدامهم البوت أول مرة.</i>")
    else:
        lines.append("<i>اضغط على اسم الطالب لإدارته أو حذفه:</i>")
        lines.append("")
        for i, s in enumerate(chunk, start + 1):
            un = (s.get("uni_id") or "").strip()
            yr = YEAR_TITLES.get(s.get("year"), "") if s.get("year") else ""
            lines.append(f"{i}. {esc(s.get('full_name') or 'بدون اسم')}"
                         + (" • <code>" + esc(un) + "</code>" if un else " • بلا رقم")
                         + (" • " + esc(yr) if yr else ""))

    # أزرار الطلاب (كل طالب في سطر)
    rows = [[btn("👤 " + _student_row_label(s), "a:stu:" + str(s.get("id")))] for s in chunk]

    # التنقّل بين الصفحات
    nav = []
    if page > 0:
        nav.append(btn("⬅️ السابق", "a:students:%d" % (page - 1)))
    if page < total_pages - 1:
        nav.append(btn("التالي ➡️", "a:students:%d" % (page + 1)))
    if nav:
        rows.append(nav)

    rows.append([btn("🔍 بحث عن طالب", "a:stusearch")])
    rows.append([btn("📊 تصدير Excel", "a:export:xlsx"), btn("📄 تصدير CSV", "a:export:csv")])
    rows.append([btn("📣 بث عام للجميع", "a:bc")])
    if total:
        rows.append([btn("🗑 حذف كل الطلاب", "a:studelall")])
    rows.append([btn("⬅️ رجوع", "a:menu"), btn("🏠 الرئيسية", "menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_student_card(record_id):
    """بطاقة طالب واحد: بياناته كاملة + زر الحذف."""
    s = db.get_student_by_id(record_id)
    if not s:
        return ("⚠️ هذا الطالب غير موجود (ربما حُذف بالفعل).",
                {"inline_keyboard": [[btn("⬅️ رجوع إلى الطلاب", "a:students"),
                                      btn("🏠 الرئيسية", "menu")]]})
    lines = [crumbs("لوحة التحكم", "الطلاب", "بطاقة طالب"),
             "👤 <b>" + esc(s.get("full_name") or "بدون اسم") + "</b>", DIVIDER, ""]
    lines.append("🔢 الرقم الجامعي: " + ("<code>" + esc(s.get("uni_id")) + "</code>" if s.get("uni_id") else "— لم يُسجَّل —"))
    lines.append("🎓 السنة: " + esc(YEAR_TITLES.get(s.get("year")) or "— لم تُحدَّد —"))
    if s.get("username"):
        lines.append("🔗 المعرف: @" + esc(s.get("username")))
    lines.append("🆔 معرّف المحادثة: <code>" + esc(str(s.get("chat_id") or "—")) + "</code>")
    lines.append("📅 انضم: " + esc(str(s.get("joined_at") or "—")[:19].replace("T", " ")))
    if s.get("updated_at"):
        lines.append("🔄 آخر تحديث: " + esc(str(s.get("updated_at") or "—")[:19].replace("T", " ")))
    lines.append("")
    lines.append("<i>إن ترك الطالب الجامعة أو الكلية، احذف سجله من هنا "
                 "فيتوقف عن استقبال التعاميم.</i>")

    rows = [[btn("🗑 حذف هذا الطالب", "a:studelask:" + str(record_id))]]
    if s.get("chat_id"):
        rows.append([btn("📣 إرسال رسالة له", "a:studm:" + str(record_id))])
    rows.append([btn("⬅️ رجوع إلى الطلاب", "a:students"), btn("🏠 الرئيسية", "menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_student_delete_confirm(record_id):
    """تأكيد الحذف — خطوة أخيرة قبل الحذف النهائي."""
    s = db.get_student_by_id(record_id)
    if not s:
        return ("⚠️ هذا الطالب غير موجود (ربما حُذف بالفعل).",
                {"inline_keyboard": [[btn("⬅️ رجوع إلى الطلاب", "a:students")]]})
    lines = [crumbs("لوحة التحكم", "الطلاب", "تأكيد الحذف"),
             "🗑 <b>تأكيد حذف الطالب</b>", DIVIDER, "",
             "👤 الاسم: <b>" + esc(s.get("full_name") or "بدون اسم") + "</b>",
             "🔢 الرقم الجامعي: <b>" + esc(s.get("uni_id") or "—") + "</b>", ""]
    lines.append("⚠️ <b>تأكيد نهائي:</b> سيُحذف سجله بالكامل ولن تصله أي رسالة "
                 "أو تعميم بعد الآن.")
    lines.append("<i>لا يمكن التراجع عن هذه الخطوة.</i>")
    rows = [
        [btn("✅ نعم، احذفه نهائيًا", "a:studelyes:" + str(record_id))],
        [btn("❌ إلغاء", "a:stu:" + str(record_id))],
    ]
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_students_search():
    """يطلب من المشرف نص البحث عن طالب."""
    lines = [crumbs("لوحة التحكم", "الطلاب", "بحث"),
             "🔍 <b>البحث عن طالب</b>", DIVIDER, "",
             "اكتب أي جزء من:",
             "• اسم الطالب",
             "• رقمه الجامعي",
             "• معرّف حسابه (ID)", ""]
    lines.append("<i>سأعرض لك كل من يطابق بحثك.</i>")
    rows = [[btn("⬅️ رجوع إلى الطلاب", "a:students"), btn("🏠 الرئيسية", "menu")]]
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_students_search_results(query):
    """نتائج البحث: كل نتيجة مع زر لإدارة الطالب."""
    results = db.find_students(query, limit=12)
    lines = [crumbs("لوحة التحكم", "الطلاب", "نتائج البحث"),
             "🔍 <b>نتائج البحث</b>", DIVIDER, "",
             "بحثك: «<b>" + esc(query) + "</b>»",
             "النتائج: <b>" + str(len(results)) + "</b>", ""]
    if not results:
        lines.append("❌ لا يوجد طالب مطابق.")
        lines.append("<i>جرّب جزءًا من الاسم أو الرقم الجامعي.</i>")
    else:
        for i, s in enumerate(results, 1):
            un = (s.get("uni_id") or "").strip()
            lines.append(f"{i}. {esc(s.get('full_name') or 'بدون اسم')}"
                         + (" • <code>" + esc(un) + "</code>" if un else ""))
    rows = [[btn("👤 " + _student_row_label(s), "a:stu:" + str(s.get("id")))] for s in results]
    rows.append([btn("🔍 بحث آخر", "a:stusearch")])
    rows.append([btn("⬅️ رجوع إلى الطلاب", "a:students"), btn("🏠 الرئيسية", "menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_students_delete_all_confirm():
    """تأكيد حذف كل سجلات الطلاب (خطوة خطيرة)."""
    total = db.count_students()
    lines = [crumbs("لوحة التحكم", "الطلاب", "حذف الكل"),
             "⚠️ <b>حذف كل سجلات الطلاب</b>", DIVIDER, "",
             "العدد الحالي: <b>" + str(total) + "</b>", ""]
    lines.append("سيُحذف <b>كل سجل طالب</b> في البوت نهائيًا.")
    lines.append("✅ <b>لن تتأثّر</b> المقررات ولا الجداول ولا التخصصات ولا معلومات الجامعة.")
    lines.append("")
    lines.append("<b>متى تحتاج هذا؟</b> في نهاية عام دراسي أو عند بداية "
                 "فصل جديد إن أردت قائمة طلاب نظيفة للفصل القادم.")
    lines.append("")
    lines.append("💾 <i>نصيحة: خذ نسخة احتياطية من «تصدير Excel» قبل الحذف.</i>")
    rows = [
        [btn("📊 تصدير Excel أولًا", "a:export:xlsx")],
        [btn("✅ نعم، احذف الكل (" + str(total) + ")", "a:studelallyes")],
        [btn("❌ إلغاء", "a:students")],
    ]
    return "\n".join(lines), {"inline_keyboard": rows}


def is_registered_student(s):
    """هل أكمل الطالب بياناته (اسم + رقم جامعي)؟"""
    return bool((s.get("full_name") or "").strip() and (s.get("uni_id") or "").strip())


def admin_broadcast_screen():
    lines = ["📣 <b>بث عام</b>", DIVIDER, "",
             "أرسل رسالة تصل إلى <b>كل الطلاب المسجّلين</b>.", "",
             "• يمكنك استخدام التنسيق: <b>عريض</b> و <i>مائل</i>.", ""]
    n = len(db.all_chat_ids())
    lines.append("عدد المستلمين الآن: <b>" + str(n) + "</b>")
    if n == 0:
        lines.append("\n⚠️ لا يوجد مستلمون بعد — سيُسجَّل الطلاب عند أول استخدام للبوت.")
    rows = [[btn("✍️ كتابة البث وإرساله", "a:bcwrite")],
            [btn("⬅️ رجوع", "a:menu"), btn("🏠 الرئيسية", "menu")]]
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_export_screen():
    lines = ["📤 <b>تصدير البيانات</b>", DIVIDER, "",
             "اختر الصيغة التي تريدها:", "",
             "• <b>Excel</b> — ملف جاهز يفتح على الجوال مباشرة.", 
             "• <b>CSV</b> — لتشغيله على أي برنامج جداول.", ""]
    lines.append("عدد الطلاب: <b>" + str(db.count_students()) + "</b>")
    rows = [[btn("📊 Excel", "a:export:xlsx"), btn("📄 CSV", "a:export:csv")],
            [btn("⬅️ رجوع", "a:menu"), btn("🏠 الرئيسية", "menu")]]
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_bot_screen():
    """شاشة تعديل مظهر البوت واسمه ونصوصه."""
    title = db.get_setting("bot_title") or db.get_setting("university_name") or "المرشد الجامعي"
    lines = ["🎨 <b>مظهر البوت وإعداداته</b>", DIVIDER, ""]
    lines.append("العنوان الحالي: <b>" + esc(title) + "</b>")
    lines.append("")
    shown = False
    for key, label in BOT_FIELDS:
        val = (db.get_setting(key) or "").strip()
        if not val:
            continue
        shown = True
        if key in ("contact_link", "channel_link"):
            lines.append("▫️ " + esc(label) + ": " + link_line(val))
        else:
            lines.append("▫️ " + esc(label) + ": " + esc(val))
    if not shown:
        lines.append("لا توجد نصوص مخصّصة بعد.")
    rows = [[btn("✏️ " + label, "a:set:" + key)] for key, label in BOT_FIELDS]
    rows.append([btn("↩️ استعادة الافتراضي", "a:resetbot")])
    rows.append([btn("⬅️ رجوع", "a:menu")])
    return "\n".join(lines), {"inline_keyboard": rows}


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
        rows.append([btn("➕ إضافة تخصص", "a:addsp")])
        specs = db.list_specs()
        for s in specs:
            rows.append([btn(f"🎯 {s['name']}", f"a:asp:{s['id']}")])
        # مقررات بلا تخصص: كانت مخفية تمامًا عن الطلاب — نُظهرها للمشرف ليصلحها
        orphans = [c for c in db.list_courses(3) if not c.get("spec_id")]
        for c in orphans:
            rows.append([btn(f"⚠️ {c['name']} (بلا تخصص)", f"a:ac:{c['id']}")])
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
        val = str(c[key] or "").strip()
        packed = unpack_tg_file(val)
        if packed:
            show = "📎 " + packed[1]
        elif val:
            show = val
        else:
            show = "—"
        lines.append(f"{label}: {esc(show) if not packed else show}")
    back = f"a:asp:{c['spec_id']}" if c["spec_id"] else f"a:y:{c['year']}"
    rows = [[btn("✏️ الاسم", f"a:ecn:{course_id}")]]
    # كل تصنيف في سطر: 📖 نظري | 💻 مخابر | 📝 دورات سابقة | الملفات
    for key, label in COURSE_FIELDS:
        mark = "✅" if (c.get(key) or "").strip() else "➕"
        rows.append([btn(f"{mark} {label}", f"a:ecl:{course_id}:{key}")])
    # في السنة الثالثة: نقل المقرر إلى تخصص آخر (أو ربطه بتخصص إن كان بلا تخصص)
    if c["year"] == 3 and db.list_specs():
        rows.append([btn("🎯 نقل إلى تخصص…", f"a:mvc:{course_id}")])
    rows.append([btn("🗑 حذف المقرر", f"a:dc:{course_id}")])
    rows.append([btn("⬅️ رجوع", back)])
    return "\n".join(lines), {"inline_keyboard": rows}


def admin_move_course_screen(course_id):
    """يختار المشرف التخصص الذي يُنقل إليه المقرر."""
    c = db.get_course(course_id)
    if not c:
        return "⚠️ غير موجود.", {"inline_keyboard": [[btn("⬅️ رجوع", "a:menu")]]}
    lines = ["🎯 <b>نقل المقرر إلى تخصص</b>", DIVIDER, "",
             "المقرر: <b>" + esc(c["name"]) + "</b>", "",
             "<i>اختر التخصص الذي ينتمي إليه هذا المقرر:</i>"]
    rows = [[btn(f"🎯 {s['name']}", f"a:setc:{course_id}:{s['id']}")] for s in db.list_specs()]
    rows.append([btn("⬅️ رجوع", f"a:ac:{course_id}")])
    return "\n".join(lines), {"inline_keyboard": rows}


def ask_for_value(chat_id, uid, action, prompt):
    save_pending(uid, action)
    send(
        chat_id,
        prompt + "\n\nأرسل القيمة الآن (أو /cancel للإلغاء، أو أرسل ‎-‎ للتفريغ).",
        {"inline_keyboard": [[btn("❌ إلغاء", "a:menu")]]},
    )


# ------------------------------------------- سجل مؤقت لخطوات التسجيل
_REGISTRY = {}


def _get_registry(uid):
    return _REGISTRY.get(uid) or {}


def _set_registry(uid, key, value):
    row = _REGISTRY.setdefault(uid, {})
    row[key] = value


def _clear_registry(uid):
    _REGISTRY.pop(uid, None)


def start_registration(chat_id, uid, username=""):
    """يبدأ تسجيل الطالب عند أول استخدام (الاسم ثم الرقم الجامعي)."""
    _REGISTRY[uid] = {"username": username or ""}
    PENDING[uid] = ("reg_name", None)
    send(chat_id,
         "👋 <b>أهلًا بك في المرشد الجامعي!</b>\n" + DIVIDER
         + "\nقبل أن نبدأ، نسجّل بياناتك مرة واحدة فقط:\n\n"
         + "✏️ <b>اكتب اسمك الكامل</b> كما في السجل الجامعي:",
         {"inline_keyboard": [[btn("⏭ لاحقًا", "skipreg")]]})


# ------------------------------------------------------------------ callbacks
def _save_student_field(chat_id, uid, field, value):
    """يحفظ حقلاً في ملف الطالب ثم يرسل إشعارًا فوريًا للإدارة."""
    try:
        old = db.get_student(uid) or {}
        if field == "year":
            db.upsert_student(uid, chat_id, year=value)
            shown = YEAR_TITLES.get(value, value)
            label = "السنة الدراسية"
        elif field == "full_name":
            db.upsert_student(uid, chat_id, full_name=value)
            shown = value
            label = "الاسم"
        elif field == "uni_id":
            db.upsert_student(uid, chat_id, uni_id=value)
            shown = value
            label = "الرقم الجامعي"
        else:
            return
        with _DIAG_LOCK:
            DIAG["students"] = db.count_students()
        send(chat_id, f"✅ تم حفظ {label}: <b>{esc(str(shown))}</b>")
        send(chat_id, *me_screen(uid))
        # إشعار فوري للمشرفين بكل تعديل
        before = old.get(field)
        notify_admins(
            "🔔 <b>تعديل بيانات طالب</b>\n"
            + DIVIDER + "\n"
            + f"👤 الطالب: {esc(old.get('full_name') or '-')}\n"
            + f"🆔 معرّفه: <code>{uid}</code>\n"
            + f"✏️ الحقل: {esc(label)}\n"
            + f"↩️ قبل: {esc(str(before) or '—')}\n"
            + f"✅ بعد: {esc(str(shown))}",
            {"inline_keyboard": [[btn("👥 بيانات الطلاب", "a:students"), btn("🏠 الرئيسية", "menu")]]})
    except Exception as exc:
        print("[student save error]", exc)
        _report_save_failure(chat_id, exc)


def handle_callback(cq):
    uid = cq["from"]["id"]
    chat_id = cq["message"]["chat"]["id"]
    data = cq.get("data", "")
    answer(cq["id"])

    try:
        if data == "menu":
            send(chat_id, *main_menu(uid))
        elif data == "me":
            send(chat_id, *me_screen(uid))
        elif data == "me:year":
            send(chat_id, *me_year_screen(uid))
        elif data.startswith("me:setyear:"):
            y = int(data.split(":")[2])
            _save_student_field(chat_id, uid, "year", y)
        elif data == "me:name":
            PENDING[uid] = ("stu_name", None)
            send(chat_id, "✏️ <b>اكتب اسمك الكامل</b> كما في السجل الجامعي:",
                 {"inline_keyboard": [[btn("❌ إلغاء", "me")]]})
        elif data == "me:uni":
            PENDING[uid] = ("stu_uni", None)
            send(chat_id, "🔢 <b>اكتب رقمك الجامعي</b> (أرقام فقط، من 4 إلى 20 رقمًا):",
                 {"inline_keyboard": [[btn("❌ إلغاء", "me")]]})
        elif data.startswith("f:"):
            _, cid, field = data.split(":", 2)
            send(chat_id, *course_file_screen(int(cid), field))
        elif data.startswith("df:"):
            _, cid, field = data.split(":", 2)
            _deliver_course_file(chat_id, int(cid), field,
                                 back_label="⬅️ رجوع إلى الملف")
        elif data.startswith("gl:"):
            parts = data.split(":")
            if parts[1] == "y":
                year = int(parts[2])
                _deliver_group_file(chat_id, "رابط مجموعة " + YEAR_TITLES.get(year, ""),
                                    db.get_year_group(year),
                                    {"inline_keyboard": [[btn("⬅️ رجوع", f"y:{year}")], go_home_row()]})
            elif parts[1] == "sp":
                sid = int(parts[2])
                spec = db.get_spec(sid)
                if spec:
                    _deliver_group_file(chat_id, "رابط مجموعة " + spec["name"], spec["group_link"],
                                        {"inline_keyboard": [[btn("⬅️ رجوع", f"sp:{sid}")], go_home_row()]})
        elif data == "me:reg":
            start_registration(chat_id, uid, (db.get_student(uid) or {}).get("username", ""))
        elif data == "skipreg":
            PENDING.pop(uid, None)
            _clear_registry(uid)
            try:
                db.upsert_student(uid, chat_id)
            except Exception:
                pass
            send(chat_id, "تمام 👌 يمكنك تسجيل بياناتك لاحقًا من:")
            send(chat_id, *main_menu(uid))
        elif data == "contact":
            send(chat_id, *contact_screen())
        elif data == "channel":
            send(chat_id, *channel_screen())
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


def do_export(chat_id, fmt="xlsx"):
    """يُجهّز ملف الطلاب ويرسله للمشرف بصيغة Excel أو CSV."""
    students = db.list_students(limit=5000)
    header = ["#", "الاسم الكامل", "الرقم الجامعي", "السنوة", "اسم المستخدم", "معرّف المحادثة", "تاريخ التسجيل"]
    rows = []
    for i, s in enumerate(students, 1):
        rows.append([
            i,
            s.get("full_name") or "",
            s.get("uni_id") or "",
            YEAR_TITLES.get(s.get("year"), "") if s.get("year") else "",
            ("@" + s["username"]) if s.get("username") else "",
            s.get("chat_id") or "",
            (s.get("created_at") or "")[:19].replace("T", " "),
        ])
    stamp = time.strftime("%Y-%m-%d")
    if fmt == "csv":
        blob = "\ufeff" + csv_text(header, rows)
        send_bytes_file(chat_id, f"students_{stamp}.csv", blob,
                        "📄 <b>قائمة الطلاب (CSV)</b>\n▫️ العدد: %d" % len(students),
                        mime="text/csv")
    else:
        try:
            blob = make_xlsx("الطلاب", header, rows)
        except Exception as exc:
            print("[xlsx error]", exc)
            send(chat_id, "⚠️ تعذّر إنشاء ملف Excel، سأرسل لك نسخة CSV بدلًا منه.")
            blob = "\ufeff" + csv_text(header, rows)
            send_bytes_file(chat_id, f"students_{stamp}.csv", blob,
                            "📄 <b>قائمة الطلاب (CSV)</b>\n▫️ العدد: %d" % len(students),
                            mime="text/csv")
            return
        send_bytes_file(chat_id, f"students_{stamp}.xlsx", blob,
                        "📊 <b>قائمة الطلاب (Excel)</b>\n▫️ العدد: %d" % len(students),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    send(chat_id, "✅ تم إنشاء الملف وإرساله. تجده في محادثة البوت كمستند يمكنك حفظه أو مشاركته.",
         {"inline_keyboard": [[btn("📤 تصدير مرة أخرى", "a:export"), btn("🏠 الرئيسية", "menu")]]})


def do_broadcast(chat_id, message):
    """يرسل رسالة لكل الطلاب المسجّلين ويخبر بالنتيجة."""
    targets = db.all_chat_ids()
    if not targets:
        send(chat_id, "⚠️ لا يوجد مستلمون بعد.")
        return
    sent = failed = 0
    for cid in targets:
        try:
            res = send(cid, message)
            if isinstance(res, dict) and res.get("ok") is False:
                failed += 1
            else:
                sent += 1
        except Exception:
            failed += 1
        time.sleep(0.05)  # احترام حدود تلغرام
    with _DIAG_LOCK:
        DIAG["broadcast_sent"] = sent
    send(chat_id,
         "📣 <b>تم إرسال البث</b>\n" + DIVIDER
         + "\n✅ وصلت إلى: <b>" + str(sent) + "</b>"
         + ("\n⚠️ تعذّر الوصول إلى: " + str(failed) if failed else "")
         + "\n\n<i>ملاحظة: من أوقف البوت (Block) لا تصله الرسائل.</i>",
         {"inline_keyboard": [[btn("📣 بث جديد", "a:bc"), btn("🏠 الرئيسية", "menu")]]})


def admin_callback(chat_id, uid, data):
    parts = data.split(":")
    kind = parts[1]

    if kind == "menu":
        send(chat_id, *admin_menu())
    elif kind == "bot":
        send(chat_id, *admin_bot_screen())
    elif kind == "resetbot":
        for key, _label in BOT_FIELDS:
            db.set_setting(key, "")
        send(chat_id, "✅ تمت استعادة المظهر الافتراضي.")
        send(chat_id, *admin_bot_screen())
    elif kind == "status":
        send(chat_id, *status_text())
    elif kind == "testdb":
        send(chat_id, *test_db_text())
    elif kind == "gen":
        send(chat_id, *admin_gen_screen())
    elif kind == "sched":
        send(chat_id, *admin_sched_screen())
    elif kind == "y":
        send(chat_id, *admin_year_screen(int(parts[2])))
    elif kind == "specs":
        send(chat_id, *admin_year_screen(3))
    elif kind == "students":
        page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        send(chat_id, *admin_students_screen(page))
    elif kind == "stu":
        send(chat_id, *admin_student_card(int(parts[2])))
    elif kind == "stusearch":
        PENDING[uid] = ("student_search", None)
        send(chat_id, *admin_students_search())
    elif kind == "studelask":
        send(chat_id, *admin_student_delete_confirm(int(parts[2])))
    elif kind == "studelyes":
        rec = int(parts[2])
        s = db.get_student_by_id(rec)
        name = (s.get("full_name") if s else "") or "بدون اسم"
        uni = (s.get("uni_id") if s else "") or "—"
        db.delete_student(rec)
        send(chat_id,
             "✅ <b>تم حذف الطالب</b>\n" + DIVIDER + "\n"
             + "👤 " + esc(name) + "\n"
             + "🔢 " + esc(uni) + "\n\n"
             + "<i>لن تصله أي رسالة أو تعميم بعد الآن.</i>")
        send(chat_id, *admin_students_screen())
    elif kind == "studelall":
        send(chat_id, *admin_students_delete_all_confirm())
    elif kind == "studelallyes":
        before = db.delete_all_students()
        send(chat_id,
             "✅ <b>تم حذف كل سجلات الطلاب</b>\n" + DIVIDER + "\n"
             + "🗑 المحذوف: <b>" + str(before) + "</b>\n\n"
             + "✅ مقرراتك وجداولك ومعلومات الجامعة <b>لم تتأثّر</b>.")
        send(chat_id, *admin_students_screen())
    elif kind == "studm":
        rec = int(parts[2])
        s = db.get_student_by_id(rec)
        if not s or not s.get("chat_id"):
            send(chat_id, "⚠️ لا يمكن مراسلة هذا الطالب (لا يوجد معرّف محادثة).")
        else:
            PENDING[uid] = ("student_msg", rec)
            send(chat_id, "✍️ <b>اكتب الرسالة</b> التي تريد إرسالها إلى «"
                 + esc(s.get("full_name") or "الطالب") + "»:",
                 {"inline_keyboard": [[btn("❌ إلغاء", "a:stu:" + str(rec))]]})
    elif kind == "export":
        fmt = parts[2] if len(parts) > 2 else ""
        if fmt in ("xlsx", "csv"):
            send(chat_id, "⏳ جارٍ تجهيز الملف…")
            do_export(chat_id, fmt)
        else:
            send(chat_id, *admin_export_screen())
    elif kind == "bc":
        send(chat_id, *admin_broadcast_screen())
    elif kind == "bcsend":
        pending = PENDING.get(uid)
        msg_text = pending[1] if (isinstance(pending, tuple) and pending[0] == "broadcast_confirm") else None
        PENDING.pop(uid, None)
        if not msg_text:
            send(chat_id, "⚠️ انتهت صلاحية الرسالة، أعد كتابتها.")
            send(chat_id, *admin_broadcast_screen())
            return
        send(chat_id, "⏳ جارٍ الإرسال… قد يستغرق قليلًا حسب عدد الطلاب.")
        do_broadcast(chat_id, msg_text)
    elif kind == "bcwrite":
        PENDING[uid] = ("broadcast", None)
        send(chat_id, "✍️ <b>اكتب الآن نص الرسالة</b> التي تريد بثّها لكل الطلاب:",
             {"inline_keyboard": [[btn("❌ إلغاء", "a:bc")]]})
    elif kind == "asp":
        send(chat_id, *admin_spec_screen(int(parts[2])))
    elif kind == "ac":
        send(chat_id, *admin_course_screen(int(parts[2])))

    # ------------------------------------------------------------ set values
    elif kind == "set":
        key = parts[2]
        label = dict(GENERAL_FIELDS + SCHEDULE_FIELDS + BOT_FIELDS)[key]
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
    elif kind == "mvc":
        send(chat_id, *admin_move_course_screen(int(parts[2])))
    elif kind == "setc":
        cid, sid = int(parts[2]), int(parts[3])
        db.update_course(cid, spec_id=sid)
        spec = db.get_spec(sid)
        send(chat_id, "✅ تم نقل المقرر إلى تخصص «" + esc(spec["name"] if spec else "-") + "».")
        send(chat_id, *admin_spec_screen(sid))
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
        save_pending(uid, ("course_link", (cid, field)))
        send(chat_id,
             f"✏️ <b>تعديل: {esc(label)}</b>\n{DIVIDER}\n\n"
             "يمكنك الآن أحد أمرين:\n"
             "📎 <b>ارفع الملف مباشرة</b> — ملف PDF/Word، صورة، مقطع فيديو، "
             "أو حتى تسجيل صوتي — وسيُحفظ ويظهر للطلاب.\n"
             "🔗 أو <b>أرسل رابطًا</b> نصيًا (Google Drive مثلاً).\n\n"
             "أو أرسل ‎-‎ للتفريغ، و /cancel للإلغاء.",
             {"inline_keyboard": [[btn("❌ إلغاء", f"a:ec:{cid}")]]})
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
    """يستقبل الملفات التي يرسلها المشرف.

    حالتان:
      1) أثناء تعديل «ملف/رابط مقرر» → نحفظ الملف المرفوع نفسه ونعيد إرساله
         للطالب لاحقًا (بلا تنزيل ولا رفع). هذا ما يجعل زر «📎 ارفع ملف» يعمل.
      2) خلاف ذلك → ملف نسخة احتياطية JSON لاستعادة المحتوى."""
    uid = msg["from"]["id"]
    chat_id = msg["chat"]["id"]
    doc = msg.get("document") or {}
    media_kind = msg.get("_media_kind") or MEDIA_DOC

    if not is_admin(uid):
        send(chat_id, "🔐 غير مسموح.")
        return

    name = doc.get("file_name", "") or "ملف"
    pending = load_pending(uid)

    # ---------- الحالة 1: المشرف يضيف ملفًا لأحد الحقول التي تعدّل الآن ----------
    if isinstance(pending, tuple) and pending[0] in ("course_link", "setting",
                                                     "year_group", "spec_link"):
        fid = doc.get("file_id")
        if not fid:
            send(chat_id, "⚠️ لم يصل الملف، حاول مرة أخرى.")
            return
        size = int(doc.get("file_size") or 0)
        if size and size > 50 * 1024 * 1024:
            hint_kb = ([[btn("⬅️ رجوع", f"a:ec:{pending[1][0]}")]]
                       if pending[0] == "course_link" else None)
            send(chat_id,
                 "⚠️ حجم الملف أكبر من 50 ميغابايت، وهو الحد الأقصى الذي يسمح به تلغرام.\n\n"
                 "💡 الحل: اضغط الملف (ZIP أو PDF مضغوط) ثم ارفعه مجددًا، "
                 "أو ضعه على Google Drive وأرسل الرابط النصي بدلًا من الملف.",
                 {"inline_keyboard": hint_kb} if hint_kb else None)
            return
        clear_pending(uid)
        kind = pending[0]
        packed = pack_tg_file(fid, name, media_kind)

        if kind == "course_link":
            cid, field = pending[1]
            label = dict(COURSE_FIELDS).get(field, "الملف")
            db.update_course(cid, **{field: packed})
            send(chat_id,
                 "✅ <b>تم حفظ الملف بنجاح!</b>\n\n"
                 f"▫️ الحقل: {esc(label)}\n"
                 f"▫️ الملف: 📎 {esc(name)}\n\n"
                 "<i>سيستطيع الطالب الآن عرض الملف وتنزيله مباشرة من داخل البوت.</i>",
                 {"inline_keyboard": [[btn("⬅️ رجوع إلى المقرر", f"a:ec:{cid}")]]})
            return
        if kind == "setting":
            db.set_setting(pending[1], packed)
            send(chat_id, "✅ <b>تم حفظ الملف بنجاح!</b>\n▫️ 📎 " + esc(name))
            if pending[1] in dict(GENERAL_FIELDS):
                send(chat_id, *admin_gen_screen())
            elif pending[1] in dict(BOT_FIELDS):
                send(chat_id, *admin_bot_screen())
            else:
                send(chat_id, *admin_sched_screen())
            return
        if kind == "year_group":
            db.set_year_group(pending[1], packed)
            send(chat_id, "✅ <b>تم حفظ الملف بنجاح!</b>\n▫️ 📎 " + esc(name))
            send(chat_id, *admin_year_screen(pending[1]))
            return
        if kind == "spec_link":
            db.update_spec(pending[1], group_link=packed)
            send(chat_id, "✅ <b>تم حفظ الملف بنجاح!</b>\n▫️ 📎 " + esc(name))
            send(chat_id, *admin_spec_screen(pending[1]))
            return

    # ---------- الحالة 2: نسخة احتياطية JSON ----------
    if not (name.endswith(".json") or doc.get("mime_type") == "application/json"):
        send(chat_id,
             "⚠️ أرسل ملف JSON فقط (ملف النسخة الاحتياطية).\n\n"
             "💡 لإضافة ملف لمقرر: افتح المقرر ← اختر الحقل ← ثم ارفع الملف هنا.")
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



def _extract_media(msg):
    """يستخرج (file_id, name, size, media) من أي وسائط أرسلها المشرف.

    يدعم: مستند، صورة، فيديو، تسجيل صوتي/مرئي. الصور تُعلَّم كـ photo كي
    تُرسَل لاحقًا بـ sendPhoto (تظهر داخل المحادثة)، وبقية الأنواع كمستند."""
    if msg.get("document"):
        d = msg["document"]
        return (d.get("file_id"), (d.get("file_name") or "ملف"),
                int(d.get("file_size") or 0), MEDIA_DOC)
    if msg.get("photo"):
        # أعلى دقة هي أكبر مقاس
        p = max(msg["photo"], key=lambda x: int(x.get("file_size") or x.get("width") or 0))
        return p.get("file_id"), "صورة", int(p.get("file_size") or 0), MEDIA_PHOTO
    for key, default in (("video", "مقطع فيديو"), ("audio", "تسجيل صوتي"),
                         ("voice", "رسالة صوتية"), ("animation", "صورة متحركة")):
        if msg.get(key):
            m = msg[key]
            name = m.get("file_name") or default
            return m.get("file_id"), name, int(m.get("file_size") or 0), MEDIA_DOC
    return None, "ملف", 0, MEDIA_DOC


def handle_media(msg):
    """يحفظ الوسائط التي يرفعها المشرف في الحقل الذي يُعدّله الآن.

    يشمل: الصور، الفيديوهات، وتسجيلات المحاضرات الصوتية — وهذا مهم
    لأن حقل «🎧 تسجيلات المحاضرات» يُرفع فيه غالبًا ملف صوت أو فيديو."""
    uid = msg["from"]["id"]
    chat_id = msg["chat"]["id"]
    if not is_admin(uid):
        send(chat_id, "🔐 غير مسموح.")
        return

    # نتصرّف كما لو كان مستندًا: نبني رسالة متوافقة ونمرّرها للمعالج نفسه
    fid, name, size, media = _extract_media(msg)
    if not fid:
        send(chat_id, "⚠️ لم يصل الملف، حاول مرة أخرى.")
        return
    synthetic = {
        "from": msg["from"],
        "chat": msg["chat"],
        "document": {"file_id": fid, "file_name": name,
                     "mime_type": "", "file_size": size},
        "_is_media": True,
        "_media_kind": media,
    }
    handle_document(synthetic)


def handle_text(msg):
    uid = msg["from"]["id"]
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if text.startswith("/cancel"):
        clear_pending(uid)
        send(chat_id, "تم الإلغاء.", {"inline_keyboard": [[btn("🏠 القائمة", "menu")]]})
        return

    pending = load_pending(uid)
    clear_pending(uid)
    if not pending:
        send(chat_id, "استخدم القائمة 👇", {"inline_keyboard": [[btn("🏠 القائمة الرئيسية", "menu")]]})
        return

    # ---------- تسجيل الطالب وتعديل بياناته (لا يحتاج صلاحية مشرف) ----------
    if isinstance(pending, tuple) and pending[0] in ("reg_name", "reg_uni", "stu_name", "stu_uni"):
        kind = pending[0]
        if text == "-":
            send(chat_id, "⚠️ لا يمكن ترك هذا الحقل فارغًا.",
                 {"inline_keyboard": [[btn("🏠 القائمة الرئيسية", "menu")]]})
            return
        if kind in ("reg_name", "stu_name"):
            ok, res = valid_name(text)
            if not ok:
                PENDING[uid] = pending
                send(chat_id, "❌ " + res + "\n\n<i>أعد كتابة الاسم:</i>")
                return
            if kind == "reg_name":
                _set_registry(uid, "full_name", res)
                PENDING[uid] = ("reg_uni", None)
                send(chat_id, "✅ تم حفظ الاسم.\n\n🔢 <b>الآن اكتب رقمك الجامعي</b> "
                              "(أرقام فقط، من 4 إلى 20 رقمًا):")
            else:
                _save_student_field(chat_id, uid, "full_name", res)
            return
        if kind in ("reg_uni", "stu_uni"):
            ok, res = valid_uni_id(text)
            if not ok:
                PENDING[uid] = pending
                send(chat_id, "❌ " + res + "\n\n<i>أعد كتابة الرقم الجامعي:</i>")
                return
            if kind == "reg_uni":
                _set_registry(uid, "uni_id", res)
                data_reg = _get_registry(uid)
                existing = db.get_student(uid) or {}
                try:
                    db.upsert_student(uid, chat_id, full_name=data_reg.get("full_name", ""),
                                      uni_id=res,
                                      username=(data_reg.get("username") or existing.get("username") or ""),
                                      year=(existing.get("year") or None))
                except Exception:
                    pass
                _clear_registry(uid)
                with _DIAG_LOCK:
                    DIAG["students"] = db.count_students()
                send(chat_id, "🎉 <b>تم تسجيلك بنجاح!</b>\n" + DIVIDER
                     + "\n👤 " + esc(data_reg.get("full_name", ""))
                     + "\n🔢 " + esc(res)
                     + "\n\nاستخدم القائمة للتنقّل 👇")
                send(chat_id, *main_menu(uid))
                notify_admins(
                    "🆕 <b>طالب جديد سجّل بياناته</b>\n" + DIVIDER + "\n"
                    + "👤 " + esc(data_reg.get("full_name", "")) + "\n"
                    + "🔢 " + esc(res) + "\n"
                    + "🆔 <code>" + str(uid) + "</code>",
                    {"inline_keyboard": [[btn("👥 بيانات الطلاب", "a:students")]]})
            else:
                _save_student_field(chat_id, uid, "uni_id", res)
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

    # كل عمليات الحفظ محميّة: أي فشل في التخزين يُبلَّغ عنه صراحة
    # بدل أن يصمت البوت بلا رد (وهذا كان سبب اختفاء رسالة «تم الحفظ»).
    try:
        value = "" if text == "-" else text
        kind, meta = (pending if isinstance(pending, tuple) else (pending, None))

        if kind == "student_search":
            send(chat_id, *admin_students_search_results(value if value else "—"))
            return
        if kind == "student_msg":
            rec = meta
            s = db.get_student_by_id(rec)
            if not s or not s.get("chat_id"):
                send(chat_id, "⚠️ تعذّر الإرسال: الطالب غير موجود.")
                return
            res = send(s["chat_id"], value)
            ok = not (isinstance(res, dict) and res.get("ok") is False)
            if ok:
                send(chat_id, "✅ تم إرسال الرسالة إلى «"
                     + esc(s.get("full_name") or "الطالب") + "».",
                     {"inline_keyboard": [[btn("⬅️ رجوع إلى الطلاب", "a:students")]]})
            else:
                send(chat_id, "⚠️ لم تُرسل الرسالة. غالبًا الطالب أوقف البوت (Block)، "
                              "أو أن سجله قديم بلا محادثة صالحة.",
                     {"inline_keyboard": [[btn("🗑 حذف هذا الطالب", "a:studelask:" + str(rec))],
                                          [btn("⬅️ رجوع إلى الطلاب", "a:students")]]})
            return
        if kind == "broadcast":
            if not text.strip():
                send(chat_id, "⚠️ الرسالة فارغة، أعد المحاولة.")
                return
            PENDING[uid] = ("broadcast_confirm", text)
            preview = text[:300] + ("…" if len(text) > 300 else "")
            send(chat_id,
                 "📣 <b>معاينة رسالة البث</b>\n" + DIVIDER + "\n" + preview
                 + "\n" + DIVIDER + "\n👥 سيصل إلى: <b>" + str(len(db.all_chat_ids())) + "</b> مستلم\n\nهل تريد الإرسال؟",
                 {"inline_keyboard": [
                     [btn("✅ إرسال الآن", "a:bcsend"), btn("❌ إلغاء", "a:bc")],
                 ]})
            return
        if kind == "broadcast_confirm":
            # وصل نص قديم بعد الإرسال، نتجاهله بهدوء
            return
        if kind == "setting":
            db.set_setting(meta, value)
            send(chat_id, f"✅ تم الحفظ: {esc(value) or '—'}")
            if meta in dict(GENERAL_FIELDS):
                send(chat_id, *admin_gen_screen())
            elif meta in dict(BOT_FIELDS):
                send(chat_id, *admin_bot_screen())
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
    except Exception as exc:
        with _DIAG_LOCK:
            DIAG["last_db_error"] = "%s: %s" % (type(exc).__name__, exc)
        print("[save error]", type(exc).__name__, exc)
        _report_save_failure(chat_id, exc)



def _db_hint(exc):
    """يحوّل خطأ التخزين إلى تلميح عملي بالعربية."""
    msg = str(exc)
    if "does not exist" in msg or "404" in msg or "PGRST" in msg or "relation" in msg:
        return ("الجداول غير موجودة في قاعدة البيانات السحابية. "
                "افتح Supabase ← SQL Editor والصق كود إنشاء الجداول مرة واحدة ثم Run.")
    if "401" in msg or "403" in msg or "Invalid API key" in msg or "JWT" in msg:
        return ("مفتاح Supabase غير صحيح. تأكد أنك نسخت مفتاح service_role "
                "(وليس anon public) ووضعته في Render → Environment باسم SUPABASE_KEY.")
    if "paused" in msg.lower() or "is not active" in msg.lower() or "project" in msg.lower() and "not found" in msg.lower():
        return ("يبدو أن مشروع Supabase مُوقوف مؤقتًا (الخطة المجانية توقف "
                "المشاريع الخاملة). افتح supabase.com ← مشروعك ← اضغط Restore "
                "أو Resume، ثم أعد المحاولة.")
    if "getaddrinfo" in msg or "Name or service not known" in msg or "timed out" in msg or "Temporary failure" in msg:
        return ("تعذّر الاتصال بالإنترنت من الخدمة. تحقّق من SUPABASE_URL في Render.")
    if "readonly" in msg.lower() or "unable to open" in msg.lower() or "disk" in msg.lower():
        return ("مساحة التخزين المؤقتة ممتلئة. اربط Supabase لتصبح البيانات دائمة.")
    return "تحقّق من متغيّرات SUPABASE_URL و SUPABASE_KEY في Render ← Environment."


def _report_save_failure(chat_id, exc):
    """يخبر المشرف أن الحفظ لم ينجح — مع السبب الأرجح وكيفية إصلاحه."""
    send(
        chat_id,
        "⚠️ <b>لم يتم الحفظ</b>\n\n"
        "💡 " + esc(_db_hint(exc)) + "\n\n"
        "<i>تفاصيل تقنية (للتشخيص):</i>\n"
        "<code>" + esc(str(exc)[:160]) + "</code>",
        {"inline_keyboard": [
            [btn("🔁 حاول مرة أخرى", "a:menu")],
            [btn("🏠 القائمة الرئيسية", "menu")],
        ]},
    )


# ------------------------------------------- تشخيص من داخل تلغرام نفسه
def status_text():
    """ملخّص الحالة يُرسل كرسالة في تلغرام — للمشرف فقط."""
    with _DIAG_LOCK:
        d = dict(DIAG)
    ok = d.get("token_ok")
    st = d.get("storage_ok")
    lines = [
        "🩺 <b>حالة البوت</b>", "",
        "• التوكن: " + ("سليم ✅" if ok else ("غير صالح ❌" if ok is False else "قيد الفحص")),
        "• التخزين: <b>" + storage_label() + "</b>",
        "• فحص قاعدة البيانات: " + (
            "سليم ✅" if st is True else ("ناقص ❌" if st is False else "محلي مؤقت ⚠️")),
        "• الرسائل الواصلة: " + str(d.get("received_updates", 0)),
        "• آخر رسالة: " + str(d.get("last_received_at") or "—"),
        "• آخر رد: " + (
            "نجح ✅" if d.get("last_reply_ok") is True else
            ("فشل ❌" if d.get("last_reply_ok") is False else "—")),
    ]
    if d.get("storage_note"):
        lines.append("• ملاحظة: " + esc(str(d["storage_note"])))
    if d.get("last_db_error"):
        lines.append("• آخر خطأ تخزين: <code>" + esc(str(d["last_db_error"])[:120]) + "</code>")
    if ok is False or st is False:
        lines.append("")
        lines.append("💡 " + esc(_db_hint(RuntimeError(str(d.get("last_db_error") or d.get("storage_note") or "")))))
    text = "\n".join(lines)
    kb = {"inline_keyboard": [
        [btn("🔍 فحص التخزين الآن", "a:testdb")],
        [btn("💾 نسخة احتياطية", "a:backup"), btn("🏠 القائمة", "menu")],
    ]}
    return text, kb


def test_db_text():
    """يجري اختبارًا حقيقيًا على التخزين ويشرح النتيجة بالعربية."""
    lines = ["🔍 <b>اختبار التخزين</b>", "", "• الوضع: <b>" + storage_label() + "</b>", ""]
    problems = []
    for table in TABLES:
        try:
            _select(table, limit=1)
            lines.append("  ✅ قراءة " + table)
        except Exception as exc:
            problems.append((table, "قراءة", exc))
            lines.append("  ❌ " + table + " — غير متاح")
    # اختبار الكتابة (إضافة صف تجريبي ثم حذفه)
    try:
        _upsert("settings", {"key": "__test__", "value": "1"}, ["key"])
        try:
            _delete("settings", {"key": "__test__"})
        except Exception:
            pass
        lines.append("")
        lines.append("  ✅ الكتابة والقراءة تعمل — الحفظ سليم <b>100%</b>")
        lines.append("")
        lines.append("🎉 جرّب الآن تعديل أي قيمة وستظهر «تم الحفظ».")
    except Exception as exc:
        problems.append(("settings", "كتابة", exc))
        lines.append("")
        lines.append("  ❌ الكتابة فشلت (الحفظ لا يعمل).")
        lines.append("")
        lines.append("💡 " + esc(_db_hint(exc)))
        lines.append("")
        lines.append("<i>تفاصيل تقنية:</i>\n<code>" + esc(str(exc)[:160]) + "</code>")
    with _DIAG_LOCK:
        if problems:
            DIAG["last_db_error"] = "%s: %s" % (type(problems[0][2]).__name__, problems[0][2])
        else:
            DIAG["last_db_error"] = None
            DIAG["storage_ok"] = True if USE_SUPABASE else None
    kb = {"inline_keyboard": [
        [btn("🔁 أعد الفحص", "a:testdb"), btn("🛠 لوحة التحكم", "a:menu")],
    ]}
    return "\n".join(lines), kb

# --------------------------------------------------------------------- update
def help_text():
    title = db.get_setting("bot_title") or "المرشد الجامعي"
    return (
        "❓ <b>كيف تستخدم " + html.escape(title) + "؟</b>\n"
        + DIVIDER + "\n\n"
        "افتح القائمة الرئيسية بزر <b>🏠 القائمة</b> في أي وقت، ثم اختر ما تريد:\n\n"
        "📢 <b>المعلومات العامة والتعاميم</b> — كل ما يخصّ الجامعة من إعلانات وروابط.\n"
        "📅 <b>الجداول الدراسية والامتحانية</b> — مباشرةً من المصدر الرسمي.\n"
        "📘📗📙📕 <b>السنوات الأربع</b> — مقررات كل سنة وروابط مجموعاتها.\n"
        "🎯 <b>التخصصات</b> — تخصصات السنة الثالثة وتفاصيلها.\n"
        "🔎 <b>بحث عن مقرر</b> — اكتب أي جزء من اسم المقرر وسيظهر لك.\n"
        "⚙️ <b>إعداداتي</b> — عدّل اسمك ورقمك الجامعي وسنتك في أي وقت.\n"
        "📞 <b>تواصل معنا</b> — للاستفسارات والدعم.\n\n"

        + DIVIDER + "\n"
        "⌨️ <b>الأوامر</b>\n"
        "/start — القائمة الرئيسية\n"
        "/help — هذه المساعدة\n"
        "/id — عرض معرّف حسابك\n"
        "/cancel — إلغاء أي إدخال\n\n"
        "🛠 <i>للمشرف فقط:</i>\n"
        "/admin — لوحة التحكم (تعديل كل شيء من داخل تلغرام)\n"
        "/broadcast — إرسال تعميم لكل الطلاب\n"
        "/export — تنزيل بيانات الطلاب (Excel أو CSV)\n"
        "/students — إدارة الطلاب (عرض/بحث/حذف طالب ترك الجامعة)\n"
        "/status — حالة البوت\n"
        "/testdb — فحص التخزين"
    )


def handle_update(update):
    try:
        with _DIAG_LOCK:
            DIAG["received_updates"] = DIAG.get("received_updates", 0) + 1
            DIAG["last_update_id"] = update.get("update_id")
            DIAG["last_received_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if "message" in update:
            msg = update["message"]
            uid = msg["from"]["id"]
            chat_id = msg["chat"]["id"]

            if msg.get("document"):
                handle_document(msg)
                return
            if msg.get("photo") or msg.get("video") or msg.get("audio") \
                    or msg.get("voice") or msg.get("animation"):
                handle_media(msg)
                return

            text = (msg.get("text") or "").strip()

            if text.startswith("/start"):
                username = (msg.get("from", {}) or {}).get("username", "")
                known = None
                try:
                    known = db.get_student(uid)
                except Exception:
                    known = None
                if ASK_STUDENT_DATA and known is None:
                    start_registration(chat_id, uid, username)
                else:
                    try:
                        db.upsert_student(uid, chat_id, username=username)
                    except Exception as exc:
                        print("[student touch warning]", exc)
                    send(chat_id, *main_menu(uid))
            elif text.startswith("/help"):
                send(chat_id, help_text())
            elif text.startswith("/id"):
                send(chat_id, f"🆔 معرّفك: <code>{uid}</code>")
            elif text.startswith("/status"):
                if is_admin(uid):
                    send(chat_id, *status_text())
                else:
                    send(chat_id, "🔐 هذا الأمر للمشرفين فقط.")
            elif text.startswith("/testdb"):
                if is_admin(uid):
                    send(chat_id, *test_db_text())
                else:
                    send(chat_id, "🔐 هذا الأمر للمشرفين فقط.")
            elif text.startswith("/broadcast"):
                if is_admin(uid):
                    send(chat_id, *admin_broadcast_screen())
                else:
                    send(chat_id, "🔐 هذا الأمر للمشرفين فقط.")
            elif text.startswith("/export"):
                if is_admin(uid):
                    fmt = "csv" if "csv" in text.lower() else "xlsx"
                    send(chat_id, "⏳ جارٍ تجهيز الملف…")
                    do_export(chat_id, fmt)
                else:
                    send(chat_id, "🔐 هذا الأمر للمشرفين فقط.")
            elif text.startswith("/students"):
                if is_admin(uid):
                    send(chat_id, *admin_students_screen())
                else:
                    PENDING[uid] = ("login_students", None)
                    send(chat_id, "🔐 أرسل كلمة مرور المشرف لعرض الطلاب:")
            elif text.startswith("/admin"):
                if is_admin(uid):
                    send(chat_id, *admin_menu())
                else:
                    PENDING[uid] = ("login", None)
                    send(chat_id, "🔐 أرسل كلمة مرور المشرف:")
            elif isinstance(PENDING.get(uid), tuple) and PENDING[uid][0] in ("login", "login_students"):
                login_kind = PENDING[uid][0]
                PENDING.pop(uid, None)
                if text == ADMIN_PASSWORD:
                    ADMIN_STATE.add(uid)
                    send(chat_id, "✅ تم تسجيل الدخول كمشرف.")
                    send(chat_id, *(admin_students_screen() if login_kind == "login_students" else admin_menu()))
                else:
                    send(chat_id, "❌ كلمة المرور غير صحيحة.")
            else:
                handle_text(msg)
        elif "callback_query" in update:
            handle_callback(update["callback_query"])
    except Exception as exc:  # pragma: no cover
        print("[update error]", exc)


# ------------------------------------------------------------------- webhook mode
def check_token():
    """يتأكد من صحة التوكن ويعرض حالة واضحة بالعربية."""
    if not BOT_TOKEN:
        with _DIAG_LOCK:
            DIAG["token_ok"] = False
            DIAG["token_message"] = "لم يُضبط BOT_TOKEN في متغيرات البيئة"
        print("❌ لم يُضبط BOT_TOKEN. أضِفه في Environment على Render.")
        return False
    info = api("getMe")
    ok = bool(info.get("ok"))
    msg = "التوكن صالح ✅" if ok else ("التوكن غير صالح ❌ (" + str(info.get("description", "سبب غير معروف")) + ")")
    user = (info.get("result") or {}).get("username")
    with _DIAG_LOCK:
        DIAG["token_ok"] = ok
        DIAG["token_message"] = msg
        DIAG["bot_username"] = user
    print(("✅" if ok else "❌"), msg, ("| البوت: @" + user) if user else "")
    if not ok:
        print("   ➜ الحل: @BotFather ← /revoke ← انسخ التوكن الجديد")
        print("            ثم Render ← Environment ← BOT_TOKEN ← الصق ← Save")
    return ok


def refresh_webhook_info():
    try:
        info = api('getWebhookInfo').get('result', {}) or {}
        with _DIAG_LOCK:
            DIAG['webhook_url'] = (info.get('url') or '(لا يوجد)')
            DIAG['pending_count'] = info.get('pending_update_count', 0)
            DIAG['last_webhook_error'] = info.get('last_error_message') or None
    except Exception as exc:
        print('[webhook info warning]', exc)


def status_page():
    refresh_webhook_info()
    with _DIAG_LOCK:
        d = dict(DIAG)

    def row(label, value, good=None):
        color = '#1a7f37' if good is True else ('#b42318' if good is False else '#333')
        return ("<tr><td class='k'>" + str(label) + "</td><td class='v' style='color:"
                + color + "'>" + str(value) + "</td></tr>")

    rows = [
        row('نوع التخزين', storage_label(), USE_SUPABASE),
        row('فحص قاعدة البيانات', d.get('storage_note') or 'قيد الفحص', d.get('storage_ok')),
        row('آخر خطأ في التخزين', d.get('last_db_error') or 'لا يوجد'),
        row('وقت التشغيل', d.get('started_at') or '-'),
        row('وضع العمل', d.get('mode') or '-'),
        row('حالة التوكن', d.get('token_message') or 'قيد الفحص', d.get('token_ok')),
        row('اسم البوت', ('@' + d['bot_username']) if d.get('bot_username') else '-', d.get('token_ok')),
        row('الرابط المسجَّل عند تلغرام', d.get('webhook_url') or '-'),
        row('تحديثات معلّقة', d.get('pending_count') if d.get('pending_count') is not None else '-'),
        row('آخر خطأ من تلغرام', d.get('last_webhook_error') or 'لا يوجد'),
        row('عدد الرسائل الواصلة', d.get('received_updates', 0)),
        row('آخر رسالة وصلت', d.get('last_received_at') or 'لم تصل أي رسالة بعد'),
        row('حالة آخر رد',
            ('نجح ✅' if d.get('last_reply_ok') is True else
             (('فشل ❌ — ' + str(d.get('last_reply_error'))) if d.get('last_reply_ok') is False else 'لم يُرسل بعد')),
            d.get('last_reply_ok')),
    ]

    if d.get('storage_ok') is False:
        cls = 'bad'
        tip = ('قاعدة البيانات السحابية (Supabase) متصلة لكن جداولها ناقصة: '
               + str(d.get('storage_note') or '') + '. افتح Supabase ← SQL Editor '
               'والصق كود إنشاء الجداول مرة واحدة ثم أعد التشغيل.')
    elif USE_SUPABASE and d.get('storage_ok') is True:
        cls = 'good'
        tip = 'البوت متصل بقاعدة Supabase — بياناتك محفوظة للأبد ولن تُفقد عند إعادة التشغيل.'
    elif d.get('token_ok') is False:
        cls = 'bad'
        tip = ('السبب الأرجح لتوقف الرد: التوكن غير صالح. افتح @BotFather ← /revoke'
               ' ← اختر بوتك ← انسخ التوكن الجديد ← Render ← Environment'
               ' ← عدّل BOT_TOKEN ← Save.')
    elif d.get('received_updates', 0) == 0:
        cls = 'warn'
        tip = ('لم تصل أي رسالة بعد. أرسل /start في تلغرام (مرتين إن كانت'
               ' الخدمة نائمة) ثم حدّث هذه الصفحة.')
    else:
        cls = 'good'
        tip = 'البوت يستقبل الرسائل ويردّ. كل شيء يعمل.'

    css = ("body{font-family:-apple-system,Segoe UI,Tahoma,sans-serif;background:#f6f7f9;margin:0;padding:16px}"
           ".card{max-width:640px;margin:0 auto;background:#fff;border-radius:14px;padding:18px;box-shadow:0 2px 10px rgba(0,0,0,.06)}"
           "h2{margin:0 0 4px}.sub{color:#777;margin:0 0 14px;font-size:14px}"
           "table{width:100%;border-collapse:collapse;font-size:15px}"
           "td{padding:8px;border-bottom:1px solid #eee}.k{color:#555}.v{font-weight:600}"
           ".tip{padding:12px;border-radius:8px;margin:14px 0;line-height:1.7}"
           ".good{background:#f0fff4;border:1px solid #b7ebc6;color:#1a7f37}"
           ".warn{background:#fffbe6;border:1px solid #ffe58f;color:#874d00}"
           ".bad{background:#fff4f4;border:1px solid #ffc9c9;color:#b42318}")

    html = ("<!DOCTYPE html><html lang='ar' dir='rtl'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>حالة البوت</title><style>" + css + "</style></head><body>"
            "<div class='card'><h2>حالة بوت المرشد الجامعي</h2>"
            "<p class='sub'>صفحة تشخيص مباشرة — حدّثها بعد إرسال /start</p>"
            "<div class='tip " + cls + "'>" + tip + "</div>"
            "<table>" + ''.join(rows) + "</table>"
            "<p style='color:#999;font-size:12px;margin-top:14px'>"
            "لا تُعرض هنا أي أسرار أو مفاتيح.</p></div></body></html>")
    return html.encode('utf-8')


def set_webhook():
    """يسجّل الويب هوك، ويضمن دائماً تطابق الرابط والتوقيع السري مع تلغرام."""
    url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
    # (1) نتحقق أولاً من الحالة المسجَّلة عند تلغرام
    try:
        info = api("getWebhookInfo").get("result", {}) or {}
        current = (info.get("url") or "").strip()
        print("🔎 الويب هوك المسجَّل عند تلغرام:", current or "(لا يوجد)")
    except Exception as exc:
        print("[webhook check warning]", exc)

    # (2) نعيد التسجيل دائماً: يضمن تطابق الرابط + التوقيع السري بعد أي إعادة نشر
    try:
        api("deleteWebhook", drop_pending_updates=False)
    except Exception:
        pass
    result = api(
        "setWebhook",
        url=url,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=False,
        allowed_updates=["message", "callback_query"],
    )
    print(f"🔗 تم ربط الويب هوك: {url} -> {result.get('description', result)}")
    try:
        info = api("getWebhookInfo").get("result", {}) or {}
        print("🔎 بعد التسجيل:", (info.get("url") or "(لا يوجد)"),
              "| تحديثات معلّقة:", info.get("pending_update_count", "?"),
              "| آخر خطأ:", info.get("last_error_message", "لا يوجد"))
    except Exception:
        pass


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
        # صفحة الحالة (تشخيص) + مسارات الفحص (Keep-Alive) لمنع نوم الخدمة
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path in ("/status", "/diag", "/check"):
            page = status_page()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        if path in ("/", "/health", "/healthz", "/ping", "/keepalive", "/webhook"):
            # رد JSON بسيط: يمنع Render من إنامة الخدمة عند غياب المستخدمين
            with _DIAG_LOCK:
                payload = {
                    "status": "active",
                    "message": "Bot is awake and running smoothly!",
                    "mode": DIAG.get("mode"),
                    "received_updates": DIAG.get("received_updates", 0),
                    "last_received_at": DIAG.get("last_received_at"),
                    "running_since": DIAG.get("started_at"),
                }
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._reply(200, "ok".encode())

    def do_POST(self):
        # نستقبل التحديث على أي مسار: قد يختلف المسار المسجَّل في تلغرام عن
        # المسار المحلي، وكان ذلك يرد "404" فتصل رسائل الناس ولا يردّ البوت.
        path = self.path.split("?", 1)[0]
        secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != WEBHOOK_SECRET:
            print(f"[webhook] رُفض طلب على المسار {path}: التوقيع السري غير مطابق")
            self._reply(403, "forbidden".encode())
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            update = json.loads(raw)
        except Exception as exc:
            print("[webhook parse error]", exc)
            self._reply(200)
            return
        if not isinstance(update, dict) or "update_id" not in update:
            self._reply(200)  # ليست رسالة تلغرام، نتجاهلها بهدوء
            return
        print(f"[webhook] وصل تحديث ({update.get('update_id')}) عبر المسار {path} -> يُعالج الآن ✅")
        threading.Thread(target=handle_update, args=(update,), daemon=True).start()
        self._reply(200)

    def log_message(self, fmt, *args):  # اجعل السجلز هادئة
        pass


def run_webhook():
    DIAG["mode"] = "وضع الويب هوك (Webhook)"
    db.init_db()
    restore_from_env()
    preload_cache()
    check_token()
    set_webhook()
    server = ThreadingHTTPServer((HOST, PORT), WebhookHandler)
    print(f"🌐 البوت يعمل بوضع الويب هوك على {HOST}:{PORT}{WEBHOOK_PATH}")
    print(f"🧭 المسار الذي تُستقبل عليه الرسائل: {WEBHOOK_PATH}")
    print(f"🔐 التوقيع السري مُفعَّل: {'نعم' if WEBHOOK_SECRET else 'لا'}")
    print("✅ جاهز — أرسل /start الآن")
    server.serve_forever()


def run_polling():
    DIAG["mode"] = "الاستقبال المباشر (Polling)"
    db.init_db()
    restore_from_env()
    preload_cache()
    check_token()
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


# ---------------------------------------------------------------------- الأوامر
def set_bot_profile():
    """يضبط اسم البوت ووصفه وترحيبه في واجهة تلغرام (قابل للتعطيل/التعديل)."""
    name = (os.getenv("BOT_NAME") or "المرشد الجامعي").strip()
    about = (os.getenv("BOT_ABOUT") or
             "دليلك الجامعي الشامل: المعلومات العامة، الجداول الدراسية، "
             "مقررات السنوات الأربع، والبحث عن أي مقرر — كل ذلك في مكان واحد."
             ).strip()
    short = (os.getenv("BOT_ABOUT_SHORT") or "دليلك الجامعي الشامل في مكان واحد 🎓").strip()
    calls = [
        ("setMyName", {"name": name[:64]}),
        ("setMyDescription", {"description": about[:512]}),
        ("setMyShortDescription", {"short_description": short[:120]}),
    ]
    done = 0
    for method, params in calls:
        try:
            if api(method, **params).get("ok"):
                done += 1
        except Exception as exc:  # لا نُفشل التشغيل لأي سبب
            print("[profile warning]", method, exc)
    print("🎨", "تم ضبط اسم البوت ووصفه" if done else "تعذّر ضبط اسم البوت ووصفه")


def set_my_commands():
    """يسجّل أوامر البوت بالعربية في واجهة تلغرام."""
    commands = [
        {"command": "start", "description": "🏠 القائمة الرئيسية"},
        {"command": "help", "description": "❓ تعليمات الاستخدام"},
        {"command": "id", "description": "🆔 عرض معرّف حسابك"},
        {"command": "cancel", "description": "❌ إلغاء أي إدخال حالي"},
        {"command": "admin", "description": "🛠 لوحة تحكم المشرف"},
        {"command": "broadcast", "description": "📣 بث عام لكل الطلاب"},
        {"command": "export", "description": "📤 تصدير بيانات الطلاب"},
    ]
    result = api("setMyCommands", commands=commands,
                 scope={"type": "default"},
                 language_code="ar")
    print("⌨️", "تم تسجيل أوامر البوت" if result.get("ok") else "تعذّر تسجيل الأوامر")


# ------------------------------------------------------------------- main loop
def main():
    if not BOT_TOKEN:
        raise SystemExit(
            "⚠️ لم يتم ضبط BOT_TOKEN.\n"
            "شغّل البوت بالشكل: BOT_TOKEN=xxxx python bot.py"
        )
    # تهيئة قاعدة البيانات والتخزين المحلي قبل أي شيء
    try:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    except Exception:
        pass
    try:
        db.init_db()
        restore_from_env()
    except Exception as exc:
        print("[db init warning]", exc)
    try:
        set_bot_profile()
        set_my_commands()
    except Exception as exc:  # لا نسمح لأي خطأ هنا بمنع تشغيل البوت
        print("[profile/commands warning]", exc)
    if WEBHOOK_URL:
        run_webhook()
    else:
        run_polling()


if __name__ == "__main__":
    main()
