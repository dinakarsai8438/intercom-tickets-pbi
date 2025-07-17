import os, json, re, time, logging, base64, requests
from typing import Any, Dict, List

import azure.functions as func

# =====================================================================
# Configuration  (expects App Settings; falls back to placeholders)
# =====================================================================
DOC_ENDPOINT = os.environ.get("DOC_ENDPOINT", "").rstrip("/")
DOC_KEY      = os.environ.get("DOC_KEY", "")
DOC_API_VER  = os.environ.get("DOC_API_VER", "2024-07-31-preview")

MAX_LEN      = int(os.environ.get("CHUNK_MAX", "2000"))
OVERLAP      = int(os.environ.get("CHUNK_OVERLAP", "200"))
FOOT_Y_HIGH  = float(os.environ.get("FOOT_Y_HIGH", "0.85"))
FOOT_Y_LOW   = float(os.environ.get("FOOT_Y_LOW",  "0.05"))

RX_PAGE_NUM   = re.compile(r'^\s*\d+\s*$')
RX_FOOTER_TXT = re.compile(r'inspection report', re.I)


# =====================================================================
# Document Intelligence Layout API call (polling)
# =====================================================================
def di_layout(pdf_bytes: bytes) -> Dict[str, Any]:
    if not DOC_ENDPOINT or not DOC_KEY:
        raise RuntimeError("DOC_ENDPOINT/DOC_KEY not configured")

    if not pdf_bytes:
        raise ValueError("Empty PDF bytes passed to di_layout")

    url = f"{DOC_ENDPOINT}/documentIntelligence/layout/analyze?api-version={DOC_API_VER}"
    headers = {
        "Ocp-Apim-Subscription-Key": DOC_KEY,
        "Content-Type": "application/pdf",
    }

    r = requests.post(url, headers=headers, data=pdf_bytes, timeout=60)
    r.raise_for_status()

    poll = r.headers.get("Operation-Location")
    if not poll:
        raise RuntimeError("Layout submit response missing Operation-Location header")

    # Poll up to ~90s
    for _ in range(60):
        time.sleep(1.5)
        pr = requests.get(poll, headers={"Ocp-Apim-Subscription-Key": DOC_KEY}, timeout=30)
        pr.raise_for_status()
        js = pr.json()
        st = js.get("status")
        if st == "succeeded":
            return js
        if st == "failed":
            raise RuntimeError(f"Document Intelligence layout failed: {js}")

    raise TimeoutError("Document Intelligence layout analysis timed out")


# =====================================================================
# Helpers
# =====================================================================
def _y_centroid(bbox_json: str) -> float:
    try:
        poly = json.loads(bbox_json)[0]
        return sum(p["y"] for p in poly) / len(poly)
    except Exception:
        return 0.5

def _looks_like_footer(text: str, bbox_json: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    if RX_PAGE_NUM.match(t) or RX_FOOTER_TXT.search(t):
        return True
    y = _y_centroid(bbox_json)
    return y > FOOT_Y_HIGH or y < FOOT_Y_LOW

def _path(h1, h2, h3):
    return " > ".join([h for h in (h1, h2, h3) if h])

def _level(h1, h2, h3):
    if h3: return 3
    if h2: return 2
    if h1: return 1
    return 0

def _chunk_texts(texts: List[str]) -> List[str]:
    out, buf = [], ""
    for seg in texts:
        if len(buf) + len(seg) + 1 <= MAX_LEN:
            buf += seg + "\n"
        else:
            out.append(buf.strip())
            buf = buf[-OVERLAP:] + seg + "\n"
    if buf.strip():
        out.append(buf.strip())
    return out


# =====================================================================
# Parse DI JSON into a section stream
# =====================================================================
def di_json_to_sections(di_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    ar = di_json.get("analyzeResult") or {}

    # Preferred: markdown.sections
    md = ar.get("markdown") or {}
    md_secs = md.get("sections")
    if isinstance(md_secs, list):
        out = []
        for s in md_secs:
            out.append({
                "content": s.get("content", ""),
                "sections": {
                    "h1": s.get("sections", {}).get("h1"),
                    "h2": s.get("sections", {}).get("h2"),
                    "h3": s.get("sections", {}).get("h3"),
                },
                "locationMetadata": {
                    "pageNumber": s.get("pageNumber"),
                    "boundingPolygons": json.dumps(s.get("boundingPolygons", [])),
                },
            })
        return out

    # Fallback: pages->blocks
    pages = ar.get("pages") or []
    out: List[Dict[str, Any]] = []
    for p in pages:
        pgno = p.get("pageNumber")
        for b in p.get("blocks", []):
            cat = b.get("category")
            txt = b.get("content", "")
            h1 = h2 = h3 = None
            if cat == "sectionHeading":
                h1 = txt
            out.append({
                "content": txt,
                "sections": {"h1": h1, "h2": h2, "h3": h3},
                "locationMetadata": {
                    "pageNumber": pgno,
                    "boundingPolygons": json.dumps(b.get("boundingPolygons", [])),
                },
            })
    return out


# =====================================================================
# Build final chunk objects
# =====================================================================
def build_chunks(sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    usable = []
    for s in sections:
        meta = s.get("locationMetadata") or {}
        if _looks_like_footer(s.get("content"), meta.get("boundingPolygons", "[]")):
            continue
        usable.append(s)

    chunks: List[Dict[str, Any]] = []
    acc_texts: List[str] = []
    acc_h1 = acc_h2 = acc_h3 = None
    pstart = pend = None

    def flush():
        nonlocal acc_texts, acc_h1, acc_h2, acc_h3, pstart, pend
        if not acc_texts:
            return
        for t in _chunk_texts(acc_texts):
            chunks.append({
                "content": t.strip(),
                "h1": acc_h1,
                "h2": acc_h2,
                "h3": acc_h3,
                "sectionPath": _path(acc_h1, acc_h2, acc_h3),
                "sectionLevel": _level(acc_h1, acc_h2, acc_h3),
                "pageStart": pstart,
                "pageEnd": pend,
            })
        acc_texts = []
        pstart = pend = None

    for s in usable:
        h1 = s.get("sections", {}).get("h1")
        h2 = s.get("sections", {}).get("h2")
        h3 = s.get("sections", {}).get("h3")
        pg = s.get("locationMetadata", {}).get("pageNumber")

        heading_changed = any([
            (h1 and h1 != acc_h1),
            (h2 and h2 != acc_h2),
            (h3 and h3 != acc_h3),
        ])

        if heading_changed:
            flush()
            acc_h1 = h1 or acc_h1
            acc_h2 = h2 if h2 or h1 else acc_h2
            acc_h3 = h3 if h3 or h2 or h1 else acc_h3
            pstart = pg
            pend = pg
        else:
            if pstart is None and pg is not None:
                pstart = pg
            if pg is not None:
                pend = pg

        txt = s.get("content") or ""
        if heading_changed:
            head_txt = h3 or h2 or h1
            if head_txt and not txt.lstrip().startswith(head_txt):
                txt = f"{head_txt}\n{txt}"
        acc_texts.append(txt)

    flush()
    return chunks


# =====================================================================
# Azure Functions App
# =====================================================================
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_doclayout", methods=["POST"])
def http_doclayout(req: func.HttpRequest) -> func.HttpResponse:  # entrypoint
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON.", status_code=400)

    results = []
    for rec in body.get("values", []):
        rid  = rec.get("recordId")
        data = rec.get("data") or {}
        fobj = data.get("file_data") or {}

        try:
            if "data" in fobj:
                pdf_bytes = base64.b64decode(fobj["data"])
            elif "url" in fobj:
                url = fobj["url"]
                sas = fobj.get("sasToken")
                if sas:
                    sep = "&" if "?" in url else "?"
                    url = f"{url}{sep}{sas}"
                pdf_bytes = requests.get(url, timeout=60).content
            else:
                raise ValueError("file_data missing 'data' or 'url'")

            di_js    = di_layout(pdf_bytes)
            sections = di_json_to_sections(di_js)
            chunks   = build_chunks(sections)

            results.append({"recordId": rid, "data": {"chunks": chunks}})
        except Exception as ex:
            logging.exception("http_doclayout error")
            results.append({"recordId": rid, "errors": [{"message": str(ex)}]})

    return func.HttpResponse(
        json.dumps({"values": results}, ensure_ascii=False),
        mimetype="application/json",
        status_code=200,
    )
