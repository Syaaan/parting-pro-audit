# ============================================================
# AFTERCARE AI – ONBOARDING AUTOMATION (Python Port)
# Internal Use Only – Do Not Share Directly with Clients
# ============================================================

from __future__ import annotations

import base64
import json
import re
import time
from dataclasses import dataclass, field
from queue import Queue
from typing import Optional
from urllib.parse import quote, urlencode

import requests

# ── Config helper ─────────────────────────────────────────────

def _cfg(key: str) -> str:
    """Read from st.secrets first, fall back to environment variables."""
    try:
        import streamlit as st
        v = st.secrets.get(key)
        if v:
            return str(v)
    except Exception:
        pass
    import os
    return os.environ.get(key, "")


# ── Constants (overridable via secrets/env) ───────────────────

def _AIRTABLE_API_KEY() -> str:
    return _cfg("AIRTABLE_API_KEY")

def _BASE1_ID() -> str:
    return _cfg("BASE1_ID") or "appbXFzZnhij88tnQ"

def _BASE1_TABLE_ID() -> str:
    return _cfg("BASE1_TABLE_ID") or "tblpf0cxsWb6Adgve"

def _BASE2_ID() -> str:
    return _cfg("BASE2_ID") or "appoDQDrqyvyPsZTY"

def _BASE2_TABLE_ID() -> str:
    return _cfg("BASE2_TABLE_ID") or "tblpf0cxsWb6Adgve"

def _TWILIO_ACCOUNT_SID() -> str:
    return _cfg("TWILIO_ACCOUNT_SID")

def _TWILIO_AUTH_TOKEN() -> str:
    return _cfg("TWILIO_AUTH_TOKEN")

def _TWILIO_TEMPLATE_FLOW_SID() -> str:
    return _cfg("TWILIO_TEMPLATE_FLOW_SID")

def _JOTFORM_API_KEY() -> str:
    return _cfg("JOTFORM_API_KEY")

def _REVIEW_FORM_TEMPLATE_ID() -> str:
    return _cfg("REVIEW_FORM_TEMPLATE_ID") or "250925621600045"

def _GOOGLE_API_KEY() -> str:
    return _cfg("GOOGLE_API_KEY")

TWILIO_STUDIO = "https://studio.twilio.com/v2"
JOTFORM_API   = "https://api.jotform.com"


def _twilio_api() -> str:
    return f"https://api.twilio.com/2010-04-01/Accounts/{_TWILIO_ACCOUNT_SID()}"


def _at_headers() -> dict:
    return {
        "Authorization": f"Bearer {_AIRTABLE_API_KEY()}",
        "Content-Type": "application/json",
    }


def _twilio_auth() -> str:
    sid   = _TWILIO_ACCOUNT_SID()
    token = _TWILIO_AUTH_TOKEN()
    return "Basic " + base64.b64encode(f"{sid}:{token}".encode()).decode()


# ══════════════════════════════════════════════════════════════
# StepContext
# ══════════════════════════════════════════════════════════════

@dataclass
class StepContext:
    output_q: Queue
    answer_q: Queue

    # ── low-level emit / ask ──────────────────────────────────

    def _put(self, msg: dict) -> None:
        self.output_q.put(msg)

    def log(self, msg: str) -> None:
        self._put({"t": "log", "m": msg})

    def ask(self, question: str) -> str:
        self._put({"t": "ask", "q": question})
        answer = self.answer_q.get()
        if answer == "__STOP__":
            raise InterruptedError("Step cancelled")
        return answer

    # ── convenience helpers ───────────────────────────────────

    def verified(self, question: str) -> None:
        """Loop until the user confirms 'y'."""
        while True:
            ans = self.ask(f"✔  {question} (y/n)")
            if ans.strip().lower() == "y":
                return
            self.warn("Please complete this before continuing.")

    def confirm(self, msg: str) -> bool:
        ans = self.ask(f"{msg} (y/n)")
        return ans.strip().lower() == "y"

    def link(self, label: str, url: str) -> None:
        self.log(f"🔗 {label}:\n   {url}")

    def instruction(self, msg: str) -> None:
        self.log(f"👉 {msg}")

    def info(self, msg: str) -> None:
        self.log(f"ℹ️  {msg}")

    def success(self, msg: str) -> None:
        self.log(f"✅ {msg}")

    def warn(self, msg: str) -> None:
        self.log(f"⚠️  {msg}")

    def divider(self) -> None:
        self.log("─" * 50)

    def summary_box(self, title: str, rows: list) -> None:
        W     = 56
        hr    = "─" * W

        def trunc(v, max_len: int) -> str:
            s = str(v) if (v is not None and v != "") else "—"
            return s if len(s) <= max_len else s[: max_len - 1] + "…"

        lines = []
        lines.append(f"  ┌{hr}┐")
        lines.append(f"  │  {trunc(title, W - 2)}".ljust(W + 3) + "│")
        lines.append(f"  ├{hr}┤")
        for label, value in rows:
            l = trunc(label, 18).ljust(18)
            v = trunc(value, W - 22)
            lines.append(f"  │  {l}  {v}".ljust(W + 3) + "│")
        lines.append(f"  └{hr}┘")
        self.log("\n".join(lines))


# ══════════════════════════════════════════════════════════════
# Airtable Helpers
# ══════════════════════════════════════════════════════════════

def get_base1_onboarding_records() -> list:
    formula = quote('{Active Status:} = "Onboarding"')
    url  = f"https://api.airtable.com/v0/{_BASE1_ID()}/{_BASE1_TABLE_ID()}?filterByFormula={formula}"
    resp = requests.get(url, headers=_at_headers())
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Airtable fetch failed: {json.dumps(data)}")
    return data.get("records", [])


def get_base2_onboarding_records() -> list:
    url  = f"https://api.airtable.com/v0/{_BASE2_ID()}/{_BASE2_TABLE_ID()}"
    resp = requests.get(url, headers=_at_headers())
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Airtable fetch failed: {json.dumps(data)}")
    return data.get("records", [])


def get_base2_onboarding_records_no_sms() -> list:
    formula = quote('OR({SMS number} = "", LEN({SMS number}) < 12)')
    url  = f"https://api.airtable.com/v0/{_BASE2_ID()}/{_BASE2_TABLE_ID()}?filterByFormula={formula}"
    resp = requests.get(url, headers=_at_headers())
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Airtable fetch failed: {json.dumps(data)}")
    # Only show records that have a funeral home name
    return [r for r in data.get("records", []) if r["fields"].get("Funeral Home Name:")]


def get_fh_record(base_id: str, table_id: str, fh_name: str) -> dict:
    formula = quote(f'{{Funeral Home Name:}} = "{fh_name}"')
    url  = f"https://api.airtable.com/v0/{base_id}/{table_id}?filterByFormula={formula}"
    resp = requests.get(url, headers=_at_headers())
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Airtable lookup failed: {json.dumps(data)}")
    if not data.get("records"):
        raise RuntimeError(f'No record found for "{fh_name}" in {base_id}')
    return data["records"][0]


def patch_record(base_id: str, table_id: str, record_id: str, fields: dict) -> dict:
    url  = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    resp = requests.patch(url, headers=_at_headers(), json={"fields": fields})
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Airtable patch failed: {json.dumps(data)}")
    return data


def copy_record_to_base2(record: dict) -> dict:
    f       = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")

    # Check if already exists
    try:
        existing = get_fh_record(_BASE2_ID(), _BASE2_TABLE_ID(), fh_name)
        # If found, just warn and return
        return existing  # caller handles the warning
    except RuntimeError:
        pass  # not found — proceed to copy

    # Strip linked record fields (arrays of Airtable record IDs starting with "rec")
    def _is_linked(v) -> bool:
        return (
            isinstance(v, list)
            and len(v) > 0
            and isinstance(v[0], str)
            and v[0].startswith("rec")
        )

    fields   = {k: v for k, v in f.items() if not _is_linked(v)}
    url      = f"https://api.airtable.com/v0/{_BASE2_ID()}/{_BASE2_TABLE_ID()}"
    attempts = 0

    while attempts < 25:
        resp = requests.post(url, headers=_at_headers(), json={"fields": fields})
        data = resp.json()
        if resp.ok:
            return data

        err = data.get("error", {})
        if err.get("type") == "INVALID_VALUE_FOR_COLUMN":
            match = re.search(r'Field "(.+)" cannot accept', err.get("message", ""))
            if match:
                fields.pop(match.group(1), None)
                attempts += 1
                continue

        if err.get("type") == "ROW_DOES_NOT_EXIST":
            fields = {
                k: v for k, v in fields.items()
                if not (isinstance(v, list) and any(isinstance(x, str) and x.startswith("rec") for x in v))
            }
            attempts += 1
            continue

        raise RuntimeError(f"Failed to create record in Base 2: {json.dumps(data)}")

    raise RuntimeError("Exceeded 25 attempts copying record to Base 2.")


def mark_automation_started(ctx: StepContext, record_id: str) -> None:
    try:
        patch_record(_BASE1_ID(), _BASE1_TABLE_ID(), record_id, {"Automation Started": True})
        ctx.success("Marked Automation Started in Base 1.")
    except Exception as e:
        ctx.warn(f"Could not mark Automation Started: {e}")


# ══════════════════════════════════════════════════════════════
# Funeral Home Picker
# ══════════════════════════════════════════════════════════════

def pick_funeral_home(ctx: StepContext, records: list, label: str) -> Optional[dict]:
    if not records:
        ctx.log(f"\n  No {label} records found.")
        return None

    lines = [f"\n  {label}:\n"]
    for i, r in enumerate(records):
        f   = r["fields"]
        sms = f.get("SMS number", "")
        if sms and len(re.sub(r"\D", "", sms)) >= 11:
            sms_label = f"📱 {sms}"
        else:
            sms_label = "🆕 No SMS yet"
        lines.append(
            f"    [{i + 1}] {f.get('Funeral Home Name:', '?')} — "
            f"{f.get('City:', '?')}, {f.get('State:', '?')} ({sms_label})"
        )
    ctx.log("\n".join(lines))

    ans = ctx.ask("\n  Enter number to select (0 to go back)").strip()
    if ans == "0":
        return None
    try:
        idx = int(ans) - 1
    except ValueError:
        return None
    if idx < 0 or idx >= len(records):
        return None
    return records[idx]


# ══════════════════════════════════════════════════════════════
# Twilio Helpers
# ══════════════════════════════════════════════════════════════

def clone_and_configure_flow(ctx: StepContext, fh_name: str, forwarding_number: str) -> dict:
    """Clone the template Studio Flow, update forwarding number, and publish it."""
    # 1. Fetch template flow
    resp = requests.get(
        f"{TWILIO_STUDIO}/Flows/{_TWILIO_TEMPLATE_FLOW_SID()}",
        headers={"Authorization": _twilio_auth()},
    )
    def_data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Template flow fetch failed: {json.dumps(def_data)}")

    # 2. Normalize forwarding number: strip leading +
    normalized = forwarding_number.lstrip("+")

    # 3. Update connect-call-to widget
    definition = def_data["definition"]
    replaced   = False
    for state in definition.get("states", []):
        if state.get("type") == "connect-call-to" and state.get("properties") is not None:
            state["properties"]["to"] = normalized
            replaced = True
            ctx.info(f'Set forward_call "to" → {normalized} (widget: {state["name"]})')
    if not replaced:
        ctx.warn("No connect-call-to widget found in template — forwarding number not set.")

    # 4. Create new draft flow
    create_resp = requests.post(
        f"{TWILIO_STUDIO}/Flows",
        headers={"Authorization": _twilio_auth(), "Content-Type": "application/x-www-form-urlencoded"},
        data={"FriendlyName": fh_name, "Status": "draft", "Definition": json.dumps(definition)},
    )
    create_data = create_resp.json()
    if not create_resp.ok:
        raise RuntimeError(f"Flow clone failed: {json.dumps(create_data)}")
    ctx.info(f'Flow created: "{create_data["friendly_name"]}" ({create_data["sid"]})')

    # 5. Publish the flow
    pub_resp = requests.post(
        f"{TWILIO_STUDIO}/Flows/{create_data['sid']}",
        headers={"Authorization": _twilio_auth(), "Content-Type": "application/x-www-form-urlencoded"},
        data={"Status": "published"},
    )
    pub_data = pub_resp.json()
    if not pub_resp.ok:
        raise RuntimeError(f"Flow publish failed: {json.dumps(pub_data)}")
    ctx.success("Flow published.")

    return create_data


def _matches_first4(phone_number: str, first4: str) -> bool:
    digits = re.sub(r"\D", "", phone_number)
    local  = digits[1:] if digits.startswith("1") else digits
    return local.startswith(first4)


def search_numbers(params: dict) -> list:
    query = urlencode({"SmsEnabled": "true", "VoiceEnabled": "true", **params, "PageSize": "50"})
    resp  = requests.get(
        f"{_twilio_api()}/AvailablePhoneNumbers/US/Local.json?{query}",
        headers={"Authorization": _twilio_auth()},
    )
    data  = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Number search failed: {json.dumps(data)}")
    return data.get("available_phone_numbers", [])


def buy_number_by_city(ctx: StepContext, city: str, state: str, forwarding_number: str) -> dict:
    digits = re.sub(r"\D", "", forwarding_number)
    local  = digits[1:] if digits.startswith("1") else digits
    first4 = local[:4]
    ctx.info(f"Matching first 4 digits: {first4}")

    chosen = None
    source = ""

    # 1. Search by city + state
    ctx.info(f"Searching in {city}, {state}...")
    numbers = search_numbers({"InLocality": city, "InRegion": state})
    match   = next((n for n in numbers if _matches_first4(n["phone_number"], first4)), None)
    if match:
        chosen = match
        source = f"{city}, {state}"

    # 2. State-wide with same first 4 digits
    if not chosen:
        ctx.warn(f"No match in {city}. Trying state-wide with same first 4 digits...")
        numbers = search_numbers({"InRegion": state})
        match   = next((n for n in numbers if _matches_first4(n["phone_number"], first4)), None)
        if match:
            chosen = match
            source = f"{state} (state-wide)"

    # 3. Area code only (first 3 digits), state-wide
    if not chosen:
        area_code = first4[:3]
        ctx.warn(f"No first-4 match statewide. Trying area code {area_code} anywhere in {state}...")
        numbers = search_numbers({"InRegion": state, "AreaCode": area_code})
        if numbers:
            chosen = numbers[0]
            source = f"{state} (area code {area_code})"

    if not chosen:
        raise RuntimeError(
            f"Could not find any number starting with {first4} "
            f"or area code {first4[:3]} in {state}."
        )

    ctx.info(f"Best match: {chosen['phone_number']} ({chosen.get('locality', source)})")
    ctx.info(f"Source: {source}")

    buy_resp = requests.post(
        f"{_twilio_api()}/IncomingPhoneNumbers.json",
        headers={"Authorization": _twilio_auth(), "Content-Type": "application/x-www-form-urlencoded"},
        data={"PhoneNumber": chosen["phone_number"]},
    )
    buy_data = buy_resp.json()
    if not buy_resp.ok:
        raise RuntimeError(f"Number purchase failed: {json.dumps(buy_data)}")
    ctx.success(f"Number purchased: {buy_data['phone_number']}")
    return buy_data


def configure_number(
    ctx: StepContext,
    number_sid: str,
    flow_sid: str,
    fh_name: str,
    phone_number: str,
) -> dict:
    friendly_name = f"{fh_name} - ({phone_number})"
    resp = requests.post(
        f"{_twilio_api()}/IncomingPhoneNumbers/{number_sid}.json",
        headers={"Authorization": _twilio_auth(), "Content-Type": "application/x-www-form-urlencoded"},
        data={
            "FriendlyName": friendly_name,
            "VoiceUrl":    f"https://studio.twilio.com/v2/Flows/{flow_sid}",
            "VoiceMethod": "POST",
            "SmsUrl":      "",
            "SmsMethod":   "POST",
        },
    )
    data = resp.json()
    if not resp.ok:
        raise RuntimeError(f"Number config failed: {json.dumps(data)}")
    ctx.success(
        f'Number configured: Friendly Name → "{friendly_name}", '
        f"Voice → Studio Flow, Messaging webhook → cleared (A2P)."
    )
    return data


# ══════════════════════════════════════════════════════════════
# JotForm Helpers
# ══════════════════════════════════════════════════════════════

def jf_get(path: str):
    sep  = "&" if "?" in path else "?"
    resp = requests.get(f"{JOTFORM_API}{path}{sep}apiKey={_JOTFORM_API_KEY()}")
    data = resp.json()
    if data.get("responseCode") != 200:
        raise RuntimeError(f"JotForm {path} → {data.get('message')}")
    return data["content"]


def jf_post(path: str, params: dict):
    sep  = "&" if "?" in path else "?"
    resp = requests.post(
        f"{JOTFORM_API}{path}{sep}apiKey={_JOTFORM_API_KEY()}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=params,
    )
    data = resp.json()
    if data.get("responseCode") != 200:
        raise RuntimeError(f"JotForm POST {path} → {data.get('message')}")
    return data["content"]


def find_jotforms_by_name(substring: str) -> list:
    forms = jf_get("/user/forms?limit=1000&orderby=created_at&direction=DESC")
    return [f for f in (forms or []) if substring.lower() in f["title"].lower()]


def clone_jotform(ctx: StepContext, template_id: str, new_title: str) -> dict:
    resp = requests.post(
        f"{JOTFORM_API}/form/{template_id}/clone?apiKey={_JOTFORM_API_KEY()}"
    )
    data = resp.json()
    if data.get("responseCode") != 200:
        raise RuntimeError(f"JotForm clone failed: {data.get('message')}")
    new_id   = data["content"]["id"]
    last_err = None

    for attempt in range(5):
        wait = 2 * (attempt + 1)
        time.sleep(wait)
        try:
            jf_post(f"/form/{new_id}/properties", {"properties[title]": new_title})
            return {"id": new_id, "url": f"https://form.jotform.com/{new_id}"}
        except Exception as e:
            last_err = e
            ctx.info(f"Title update attempt {attempt + 1} failed — retrying...")

    raise last_err


def add_email_notification(form_id: str, email: str) -> None:
    settings  = jf_get(f"/form/{form_id}/settings")
    notifiers = settings.get("email") or {}
    if not notifiers:
        raise RuntimeError("No email notifier found on form.")
    first_key = next(iter(notifiers))
    notifier  = notifiers[first_key]
    existing  = notifier.get("to", "")
    if email in existing:
        return  # already set

    new_to  = f"{existing};{email}" if existing else email
    updated = {**notifier, "to": new_to}
    params  = {}
    for k, v in updated.items():
        params[f"settings[email][{first_key}][{k}]"] = (
            json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        )
    jf_post(f"/form/{form_id}/settings", params)


def update_google_review_url(form_id: str, place_id: str) -> bool:
    new_url   = f"https://search.google.com/local/writereview?placeid={place_id}"
    review_re = re.compile(
        r"https://search\.google\.com/local/writereview\?placeid=[^\"'\s\\]*"
    )

    props = jf_get(f"/form/{form_id}/properties")

    # Check thankyouLink first
    if props.get("thankyouLink") and review_re.search(props["thankyouLink"]):
        jf_post(f"/form/{form_id}/properties", {"properties[thankyouLink]": new_url})
        return True

    # Check conditions
    if props.get("conditions"):
        cond_str = json.dumps(props["conditions"])
        match    = review_re.search(cond_str)
        if match:
            updated_cond = review_re.sub(new_url, cond_str)
            jf_post(f"/form/{form_id}/properties", {"properties[conditions]": updated_cond})
            return True

    return False


# ══════════════════════════════════════════════════════════════
# Google Places Helpers
# ══════════════════════════════════════════════════════════════

def search_google_place(query: str) -> list:
    resp = requests.post(
        "https://places.googleapis.com/v1/places:searchText",
        headers={
            "Content-Type":     "application/json",
            "X-Goog-Api-Key":   _GOOGLE_API_KEY(),
            "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress",
        },
        json={"textQuery": query},
    )
    data = resp.json()
    if not resp.ok:
        err_msg = data.get("error", {}).get("message", json.dumps(data))
        raise RuntimeError(f"Google Places API error: {err_msg}")
    return [
        {
            "place_id":          p.get("id", ""),
            "name":              p.get("displayName", {}).get("text", ""),
            "formatted_address": p.get("formattedAddress", ""),
        }
        for p in data.get("places", [])
    ]


def run_place_id_lookup(ctx: StepContext, fh_name: str, address: str) -> Optional[str]:
    ctx.log(f'\n     Searching: "{address}"\n')

    candidates = []
    try:
        candidates = search_google_place(address)
    except Exception as e:
        ctx.warn(f"Google Places lookup failed: {e}")
        ctx.log("\n     You can find the Place ID manually at:")
        ctx.log(
            "     https://developers.google.com/maps/documentation/javascript/examples/places-placeid-finder\n"
        )
        manual = ctx.ask("     Enter Place ID manually (or leave blank to skip)").strip()
        return manual or None

    if not candidates:
        ctx.warn("No Google Places results found.")
        manual = ctx.ask("     Enter Place ID manually (or leave blank to skip)").strip()
        return manual or None

    lines = []
    for i, c in enumerate(candidates):
        lines.append(f"     [{i + 1}] {c['name']}")
        lines.append(f"          {c['formatted_address']}")
        lines.append(f"          Place ID: {c['place_id']}\n")
    ctx.log("\n".join(lines))

    if len(candidates) == 1:
        if ctx.confirm(f'Use this result: "{candidates[0]["name"]}"?'):
            return candidates[0]["place_id"]
        manual = ctx.ask("     Enter Place ID manually (or blank to skip)").strip()
        return manual or None

    pick = ctx.ask(
        f"     Select [1–{len(candidates)}] or enter Place ID manually"
    ).strip()
    try:
        idx = int(pick) - 1
        if 0 <= idx < len(candidates):
            return candidates[idx]["place_id"]
    except ValueError:
        pass
    return pick or None


# ══════════════════════════════════════════════════════════════
# STEP 1: Review Notification & Funeral Home Record
# ══════════════════════════════════════════════════════════════

def step1_pick_and_sync(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  ✅  STEP 1: Review Notification & Funeral Home Record")
    ctx.log("═" * 56)

    records = get_base1_onboarding_records()
    record  = pick_funeral_home(ctx, records, "Base 1 – Onboarding Funeral Homes")
    if not record:
        return

    f      = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")

    ctx.divider()
    ctx.log(f"\n  🏠  {fh_name}")
    ctx.log(f"  📍  {f.get('City:', '')}, {f.get('State:', '')} {f.get('Zip Code:', '')}")
    ctx.log(f"  📞  {f.get('Funeral Home Actual Phone Number', '')}")
    ctx.log(f"  📧  {f.get('Email Notification:', '')}")
    ctx.log(f"  👤  {f.get('Name of Primary Contact:', '')} ({f.get('Primary Contact Email:', '')})")
    stripe   = f.get("Link to Stripe Account: ", "") or "⚠️  Not set"
    hubspot  = f.get("HubSpot Record URL:", "") or "⚠️  Not set"
    timezone = f.get("Time Zone:", "") or "⚠️  Not set"
    ctx.log(f"  💳  Stripe: {stripe}")
    ctx.log(f"  🌐  HubSpot: {hubspot}")
    ctx.log(f"  🕐  Timezone: {timezone}")
    ctx.divider()

    ctx.instruction("Review all data above and confirm the funeral home qualifies.")
    ctx.verified("Have you reviewed the record and confirmed it qualifies?")

    ctx.instruction("Verify Stripe and HubSpot links are added to the Airtable record.")
    if not f.get("Link to Stripe Account: ") or f.get("Link to Stripe Account: ", "").endswith("/"):
        ctx.warn("Stripe link appears missing or incomplete — please update it in Airtable.")
    if not f.get("HubSpot Record URL:"):
        ctx.warn("HubSpot URL is missing — please update it in Airtable.")
    ctx.verified("Are Stripe and HubSpot links added to Airtable?")

    if not ctx.confirm(f'Proceed with onboarding for "{fh_name}"?'):
        ctx.log("\n  👋 Cancelled.")
        return

    if not f.get("Automation Started"):
        ctx.info("Syncing record to Base 2...")
        try:
            existing = get_fh_record(_BASE2_ID(), _BASE2_TABLE_ID(), fh_name)
            ctx.warn(f"Record already exists in Base 2 ({existing['id']}). Skipping copy.")
        except RuntimeError:
            new_rec = copy_record_to_base2(record)
            ctx.success(f"Record created in Base 2 ({new_rec['id']})")
        mark_automation_started(ctx, record["id"])
    else:
        ctx.info("Record already synced to Base 2.")

    # Reload to capture anything the user just filled in
    latest = get_fh_record(_BASE1_ID(), _BASE1_TABLE_ID(), fh_name)["fields"]
    ctx.divider()
    ctx.summary_box(f"STEP 1 SUMMARY — {fh_name}", [
        ["Funeral Home",    fh_name],
        ["Location",        f"{latest.get('City:', '')}, {latest.get('State:', '')} {latest.get('Zip Code:', '')}".strip()],
        ["Phone",           latest.get("Funeral Home Actual Phone Number", "") or "—"],
        ["Email Notif.",    latest.get("Email Notification:", "") or "—"],
        ["Primary Contact", latest.get("Name of Primary Contact:", "") or "—"],
        ["Contact Email",   latest.get("Primary Contact Email:", "") or "—"],
        ["Stripe",          latest.get("Link to Stripe Account: ", "") or "NOT SET"],
        ["HubSpot",         latest.get("HubSpot Record URL:", "") or "NOT SET"],
        ["Timezone",        latest.get("Time Zone:", "") or "—"],
        ["Base 2 Sync",     "already synced" if f.get("Automation Started") else "synced ✓"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("Please correct any issues in Airtable before continuing to Step 2.")
        return

    ctx.success("Step 1 complete!")


# ══════════════════════════════════════════════════════════════
# STEP 2: Twilio Setup (Automated)
# ══════════════════════════════════════════════════════════════

def step2_twilio(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  📞  STEP 2: Twilio Setup – Call Forwarding & Number Configuration")
    ctx.log("═" * 56)

    records = get_base2_onboarding_records_no_sms()
    record  = pick_funeral_home(ctx, records, "Base 2 – Onboarding (No SMS yet)")
    if not record:
        return

    f             = record["fields"]
    fh_name       = f.get("Funeral Home Name:", "")
    forwarding_num = f.get("Funeral Home Actual Phone Number", "")
    city          = f.get("City:", "")
    state         = f.get("State:", "")

    ctx.divider()
    ctx.info(f"Funeral Home:  {fh_name}")
    ctx.info(f"Forwarding to: {forwarding_num}")
    ctx.info(f"City/State:    {city}, {state}")
    ctx.divider()

    if not ctx.confirm(f'Run automated Twilio setup for "{fh_name}"?'):
        ctx.log("\n  👋 Cancelled.")
        return

    ctx.log("\n  1️⃣  Cloning & publishing Twilio Studio Flow...\n")
    new_flow = clone_and_configure_flow(ctx, fh_name, forwarding_num)

    ctx.log("\n  2️⃣  Buying phone number...\n")
    new_number = buy_number_by_city(ctx, city, state, forwarding_num)

    ctx.log("\n  3️⃣  Configuring number...\n")
    configure_number(ctx, new_number["sid"], new_flow["sid"], fh_name, new_number["phone_number"])

    ctx.log("\n  4️⃣  Updating Airtable...\n")
    patch_record(_BASE2_ID(), _BASE2_TABLE_ID(), record["id"], {"SMS number": new_number["phone_number"]})
    ctx.success("SMS number written to Base 2.")

    ctx.divider()
    ctx.summary_box(f"STEP 2 SUMMARY — {fh_name}", [
        ["Funeral Home",   fh_name],
        ["New SMS Number", new_number["phone_number"]],
        ["Forwards to",    forwarding_num],
        ["City / State",   f"{city}, {state}"],
        ["Studio Flow",    new_flow["sid"]],
        ["Airtable",       "Base 2 only (Texting Hub v1.3)"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("Check Twilio and Airtable manually if anything looks off.")
        return

    ctx.success("Step 2 complete!")


# ══════════════════════════════════════════════════════════════
# STEP 3: Review Form & Google Place ID (Automated)
# ══════════════════════════════════════════════════════════════

def step3_review_and_place_id(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  📝  STEP 3: Review Form & Google Place ID Setup")
    ctx.log("═" * 56)

    records = get_base2_onboarding_records()
    record  = pick_funeral_home(ctx, records, "Base 2 – Onboarding Funeral Homes")
    if not record:
        return

    f       = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")
    address = ", ".join(
        p for p in [
            fh_name,
            f.get("Funeral Home Address", ""),
            f.get("City:", ""),
            f.get("State:", ""),
        ] if p
    )
    email   = f.get("Email Notification:", "")

    ctx.info(f"Funeral Home: {fh_name}")
    ctx.info(f"Address:      {f.get('Funeral Home Address', '(not set)')}, {f.get('City:', '')}, {f.get('State:', '')}")
    ctx.info(f"Notify Email: {email or '(not set)'}")

    # ── PRE-FLIGHT ────────────────────────────────────────────
    ctx.divider()
    ctx.log("\n  🔍  PRE-FLIGHT CHECKS — scanning JotForm + Airtable...\n")

    existing_place_id    = f.get("Link for Google Review", "")
    existing_review_forms: list = []
    try:
        all_matching         = find_jotforms_by_name(fh_name)
        existing_review_forms = [jf for jf in all_matching if "review" in jf["title"].lower()]
    except Exception as e:
        ctx.warn(f"JotForm search failed: {e} — continuing without pre-flight data.")

    W    = 56
    line = "─" * W
    lines = [
        f"  ┌{line}┐",
        ("  │  PRE-FLIGHT SUMMARY".ljust(W + 3) + "│"),
        f"  ├{line}┤",
    ]
    if existing_place_id:
        lines.append(("  │  📍 Google Place ID : ALREADY SET".ljust(W + 3) + "│"))
        lines.append((f"  │     {existing_place_id[:50]}".ljust(W + 3) + "│"))
    else:
        lines.append(("  │  📍 Google Place ID : not set → will look up".ljust(W + 3) + "│"))

    if existing_review_forms:
        lines.append((f"  │  📋 Review Form     : {len(existing_review_forms)} existing form(s) found".ljust(W + 3) + "│"))
        for jf in existing_review_forms:
            lines.append((f"  │     • {jf['title'][:48]}".ljust(W + 3) + "│"))
    else:
        lines.append(("  │  📋 Review Form     : not found → will clone template".ljust(W + 3) + "│"))

    lines.append(f"  └{line}┘\n")
    ctx.log("\n".join(lines))

    if not ctx.confirm("Everything look right? Proceed with Step 3?"):
        ctx.log("\n  👋 Cancelled.")
        return

    # ── C. GOOGLE PLACE ID ────────────────────────────────────
    ctx.divider()
    ctx.log("\n  C.  GOOGLE PLACE ID\n")

    place_id = existing_place_id or None

    if existing_place_id:
        ctx.info(f"Current Place ID in Airtable: {existing_place_id}")
        if ctx.confirm("Re-lookup and overwrite with a fresh search?"):
            place_id = run_place_id_lookup(ctx, fh_name, address)
        else:
            ctx.info("Keeping existing Place ID.")
    else:
        place_id = run_place_id_lookup(ctx, fh_name, address)

    if place_id:
        ctx.success(f"Place ID: {place_id}")
    else:
        ctx.warn("No Place ID set — you can add it to Airtable manually later.")

    # ── A. REVIEW FORM ────────────────────────────────────────
    ctx.divider()
    ctx.log("\n  A.  REVIEW FORM\n")

    review_url = ""

    if existing_review_forms:
        form_lines = ["  Existing Review form(s) found:\n"]
        for i, jf in enumerate(existing_review_forms):
            form_lines.append(f"    [{i + 1}] {jf['title']}\n        https://form.jotform.com/{jf['id']}\n")
        ctx.log("\n".join(form_lines))

        if ctx.confirm("Use an existing form instead of cloning a new one?"):
            pick_idx = 0
            if len(existing_review_forms) > 1:
                try:
                    pick_idx = int(ctx.ask(f"  Enter number [1–{len(existing_review_forms)}]")) - 1
                    if pick_idx < 0 or pick_idx >= len(existing_review_forms):
                        pick_idx = 0
                except (ValueError, TypeError):
                    pick_idx = 0
            review_url = f"https://form.jotform.com/{existing_review_forms[pick_idx]['id']}"
            ctx.info(f"Using existing form: {review_url}")

    if not review_url:
        review_title = f"{fh_name} - Review Form"
        ctx.log(f'\n  Will create: "{review_title}"')
        if email:
            ctx.log(f"  Will add email notification:  {email}")
        if place_id:
            ctx.log(f"  Will set Google Review URL:   placeid={place_id}")

        if not ctx.confirm("Clone and configure Review form?"):
            ctx.warn("Review form skipped.")
        else:
            ctx.info("Cloning Review form...")
            form = clone_jotform(ctx, _REVIEW_FORM_TEMPLATE_ID(), review_title)
            ctx.success(f"Review form created: {form['url']}")
            review_url = form["url"]

            if email:
                try:
                    add_email_notification(form["id"], email)
                    ctx.success(f"Email notification added: {email}")
                except Exception as e:
                    ctx.warn(f"Could not auto-add email notification: {e}")
                    ctx.instruction(f"Manually add {email} under form Settings → Emails")

            if place_id:
                try:
                    updated = update_google_review_url(form["id"], place_id)
                    if updated:
                        ctx.success("Google Review URL updated in form conditions.")
                    else:
                        ctx.warn("Google Review URL not found in form conditions — please update manually.")
                        ctx.instruction(
                            f"Set the URL to: https://search.google.com/local/writereview?placeid={place_id}"
                        )
                except Exception as e:
                    ctx.warn(f"Could not auto-update Google Review URL: {e}")
                    ctx.instruction(
                        f"Manually set URL to: https://search.google.com/local/writereview?placeid={place_id}"
                    )

    # ── RESULTS SUMMARY & AIRTABLE UPDATE ─────────────────────
    ctx.divider()
    ctx.summary_box(f"STEP 3 SUMMARY — {fh_name}", [
        ["Funeral Home",    fh_name],
        ["Google Place ID", place_id   or "NOT SET"],
        ["Review Form URL", review_url or "not created"],
        ["Email Notif.",    email      or "—"],
        ["Airtable",        "Base 2 only (Texting Hub v1.3)"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("No changes saved. Correct any issues and re-run Step 3.")
        return

    updates: dict = {}
    if place_id:
        updates["Link for Google Review"] = place_id
    if review_url:
        updates["Review Link"] = review_url

    if not updates:
        ctx.warn("Nothing to save — all items were skipped.")
        return

    if not ctx.confirm("Save results to Airtable Base 2?"):
        ctx.instruction("Copy the values above and update Airtable manually.")
        ctx.success("Step 3 complete!")
        return

    try:
        patch_record(_BASE2_ID(), _BASE2_TABLE_ID(), record["id"], updates)
        ctx.success("Airtable Base 2 updated.")
    except Exception as e:
        ctx.warn(f"Base 2 update failed: {e}")
        ctx.instruction("Please update Base 2 manually with the values above.")

    ctx.success("Step 3 complete!")


# ══════════════════════════════════════════════════════════════
# STEP 4: Finalize Airtable Record & Set Up Zaps
# ══════════════════════════════════════════════════════════════

def step4_zapier_and_airtable(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  🧾  STEP 4: Finalize Airtable Record & Set Up Zaps")
    ctx.log("═" * 56)

    records = get_base2_onboarding_records()
    record  = pick_funeral_home(ctx, records, "Base 2 – Onboarding Funeral Homes")
    if not record:
        return

    f       = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")
    ctx.info(f"Funeral Home: {fh_name}")

    # ── A. FINALIZE AIRTABLE RECORD ───────────────────────────
    ctx.divider()
    ctx.log("\n  A.  FINALIZE AIRTABLE RECORD\n")
    ctx.log(f"     Open Base 2 for {fh_name} and verify the following fields:\n")

    stripe_val  = f.get("Link to Stripe Account: ", "") or "⚠️  MISSING — add before continuing"
    hubspot_val = f.get("HubSpot Record URL:", "")      or "⚠️  MISSING — add before continuing"
    sms_val     = f.get("SMS number", "")               or "⚠️  MISSING — run Step 2 first"
    review_val  = f.get("Review Link", "")              or "⚠️  MISSING — run Step 3 first"
    place_val   = f.get("Link for Google Review", "")   or "⚠️  MISSING — run Step 3 first"

    ctx.log(f"     [ ] Stripe ID        : {stripe_val}")
    ctx.log(f"     [ ] HubSpot URL      : {hubspot_val}")
    ctx.log(f"     [ ] SMS Number       : {sms_val}")
    ctx.log(f"     [ ] Review Link      : {review_val}")
    ctx.log(f"     [ ] Google Place ID  : {place_val}")
    ctx.log("     [ ] Internal notes   : add any relevant onboarding notes")
    ctx.log("     [ ] Message prompts  : open each prompt and verify nothing is broken\n")
    ctx.link("Open Base 2 record", "https://airtable.com/appoDQDrqyvyPsZTY")
    ctx.verified("Have you reviewed and filled in all missing fields?")

    # ── B. ZAPIER WORKFLOWS ───────────────────────────────────
    ctx.divider()
    ctx.log("\n  B.  ZAPIER WORKFLOWS\n")
    ctx.link("Zapier Folder", "https://zapier.com/app/assets/folders/2839428")
    ctx.log("")

    ctx.log("     STEP 1 — Create a new subfolder")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Open the Zapier folder link above")
    ctx.log('     • Click "New Folder" inside the main folder')
    ctx.log(f'     • Name it exactly: "{fh_name}"\n')
    ctx.verified(f'Created subfolder "{fh_name}"?')

    ctx.log("     STEP 2 — Copy Zaps into the new subfolder")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Go to an existing FH folder to find the Zaps to copy")
    ctx.log("     • For each Zap: click ••• → Duplicate")
    ctx.log(f'     • Move the duplicate into the "{fh_name}" subfolder\n')
    ctx.verified("Have you copied all required Zaps into the subfolder?")

    ctx.log("     STEP 3 — Rename & configure each Zap")
    ctx.log("     " + "─" * 45)
    ctx.log(f'     • Rename each Zap to include "{fh_name}"')
    ctx.log(f'       e.g.  "New Contact → SMS  [{fh_name}]"')
    ctx.log("     • Open each Zap and update any filters or field values")
    ctx.log("       that reference the previous funeral home's name\n")
    ctx.verified("Have you renamed and configured all Zaps?")

    ctx.log("     STEP 4 — Test each Zap")
    ctx.log("     " + "─" * 45)
    ctx.log('     • Click "Test" on each Zap and verify it runs correctly')
    ctx.log("     • Check that trigger data and actions look right\n")
    ctx.verified("Have all Zaps been tested successfully?")

    ctx.log("     STEP 5 — Enable all Zaps")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Toggle each Zap ON")
    ctx.log("     • Confirm all Zaps show status: ON\n")
    ctx.verified("Are all Zaps enabled?")

    ctx.divider()
    ctx.summary_box(f"STEP 4 SUMMARY — {fh_name}", [
        ["Funeral Home",  fh_name],
        ["Stripe",        f.get("Link to Stripe Account: ", "") or "—"],
        ["HubSpot",       f.get("HubSpot Record URL:", "") or "—"],
        ["Zapier Folder", f"{fh_name} — created"],
        ["Zaps",          "copied, renamed, tested, enabled"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("Finish any remaining Zapier setup before moving on.")
        return

    ctx.success("Step 4 complete!")


# ══════════════════════════════════════════════════════════════
# STEP 5: Build the Airtable Interface
# ══════════════════════════════════════════════════════════════

def step5_interface(ctx: StepContext) -> Optional[str]:
    ctx.log("\n" + "═" * 56)
    ctx.log("  🖥️  STEP 5: Build the Airtable Interface")
    ctx.log("═" * 56)

    records = get_base2_onboarding_records()
    record  = pick_funeral_home(ctx, records, "Base 2 – Onboarding Funeral Homes")
    if not record:
        return None

    f       = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")
    ctx.info(f"Funeral Home: {fh_name}")

    ctx.link("Base 2 Interfaces", "https://airtable.com/appoDQDrqyvyPsZTY")

    # ── A. SET UP INTERFACE ───────────────────────────────────
    ctx.divider()
    ctx.log("\n  A.  SET UP INTERFACE\n")

    ctx.log("     STEP 1 — Duplicate the template")
    ctx.log("     " + "─" * 45)
    ctx.log('     • In Base 2, go to Interfaces (left sidebar)')
    ctx.log('     • Find "TEMPLATE – DO NOT TOUCH"')
    ctx.log('     • Click the ••• menu next to it → Duplicate\n')
    ctx.verified('Have you duplicated the "TEMPLATE – DO NOT TOUCH" interface?')

    ctx.log("\n     STEP 2 — Rename it")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Click the ••• menu on the duplicated interface → Rename")
    ctx.log("     • Set the name to:\n")
    ctx.log(f"          {fh_name}\n")
    ctx.verified("Have you renamed the interface?")

    ctx.log("\n     STEP 3 — Alphabetize")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Drag the interface into alphabetical order in the list\n")
    ctx.verified("Done?")

    # ── B. ADJUST FILTERS FOR EACH VIEW ──────────────────────
    ctx.divider()
    ctx.log("\n  B.  ADJUST FILTERS — VIEW BY VIEW\n")
    ctx.log(f'     Filter value to use everywhere:  "{fh_name}"\n')
    ctx.log("     " + "─" * 45 + "\n")

    ctx.log("     [1]  Contact & Messaging Settings")
    ctx.log("          • Open this view")
    ctx.log(f'          • Left sidebar → set filter: Funeral Home = "{fh_name}"\n')
    ctx.verified("Done with Contact & Messaging Settings?")

    ctx.log("\n     [2]  Add Multiple Contacts")
    ctx.log("          • Open this view")
    ctx.log(f'          • Top margin area → set filter: Funeral Home = "{fh_name}"')
    ctx.log("          • Add a test contact, then delete it")
    ctx.log("          • Verify the funeral home auto-assigned correctly\n")
    ctx.verified("Done with Add Multiple Contacts?")

    ctx.log("\n     [3]  Add New Contact")
    ctx.log("          • Open this view")
    ctx.log('          • Find the "Funeral Home Name" field')
    ctx.log(f'          • Set default value to:  "{fh_name}"')
    ctx.log("          • Disable field editing for that field\n")
    ctx.verified("Done with Add New Contact?")

    ctx.log("\n     [4]  All Messages")
    ctx.log("          • Open this view")
    ctx.log(f'          • Left sidebar → set filter: Funeral Home = "{fh_name}"\n')
    ctx.verified("Done with All Messages?")

    ctx.log("\n     [5]  Needs Human Response")
    ctx.log("          • Open this view")
    ctx.log(f'          • Left sidebar → set filter: Funeral Home = "{fh_name}"\n')
    ctx.verified("Done with Needs Human Response?")

    ctx.log("\n     [6]  Send Manual Message")
    ctx.log("          • Open this view")
    ctx.log(f'          • Contact info block → set filter: Funeral Home = "{fh_name}"\n')
    ctx.verified("Done with Send Manual Message?")

    ctx.log("\n     [7]  Account Settings")
    ctx.log("          • Open this view")
    ctx.log(f'          • Left sidebar → set filter: Funeral Home = "{fh_name}"\n')
    ctx.verified("Done with Account Settings?")

    ctx.log("\n     [8]  Dashboard")
    ctx.log("          • Open this view")
    ctx.log(f'          • Go block by block and update each filter to "{fh_name}"\n')
    ctx.verified("Done with Dashboard?")

    # ── C. CLEAN UP & PUBLISH ─────────────────────────────────
    ctx.divider()
    ctx.log("\n  C.  CLEAN UP & PUBLISH\n")

    ctx.log("     STEP 1 — Delete empty fields")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Look for any empty prompt or timing fields")
    ctx.log("     • Delete them if they have no content\n")
    ctx.verified("Done?")

    ctx.log("\n     STEP 2 — Holiday Message Send Date")
    ctx.log("     " + "─" * 45)
    ctx.log('     • Check if "Holiday Message Send Date" exists')
    ctx.log("     • If missing → set it to:  December 1 at 1:00 PM\n")
    ctx.verified("Done?")

    ctx.log("\n     STEP 3 — Publish")
    ctx.log("     " + "─" * 45)
    ctx.log("     • Click Publish on the interface")
    ctx.log("     • Do NOT share it yet — sharing happens in Step 7\n")
    ctx.verified("Have you published the interface?")

    ctx.instruction("Copy the Interface URL and paste it below:")
    interface_url = ctx.ask("\n  🔗 Interface URL").strip()

    ctx.instruction("Add the Interface URL to Airtable Base 2 (Texting Hub v1.3)")
    ctx.verified("Done?")

    ctx.divider()
    ctx.summary_box(f"STEP 5 SUMMARY — {fh_name}", [
        ["Funeral Home",   fh_name],
        ["Interface URL",  interface_url or "—"],
        ["Template",       "duplicated + renamed"],
        ["Views (8)",      "all filters set"],
        ["Cleanup",        "empty fields removed"],
        ["Holiday Date",   "Dec 1 at 1 PM set"],
        ["Interface",      "published (not shared yet)"],
        ["Airtable",       "Base 2 only (Texting Hub v1.3)"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("Finish any remaining interface setup before moving on.")
        return interface_url

    ctx.success("Step 5 complete!")
    return interface_url


# ══════════════════════════════════════════════════════════════
# STEP 6: QA (INACTIVE — SKIP)
# ══════════════════════════════════════════════════════════════

def step6_qa(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  🧪  STEP 6: Run QA")
    ctx.log("═" * 56)
    ctx.log("  ⏭️   SKIP: Step 6 is currently INACTIVE. Skipping automatically.")
    time.sleep(1.5)


# ══════════════════════════════════════════════════════════════
# STEP 7: Share Interface & Activate Funeral Home
# ══════════════════════════════════════════════════════════════

def step7_share_and_activate(ctx: StepContext) -> None:
    ctx.log("\n" + "═" * 56)
    ctx.log("  🚀  STEP 7: Share Interface & Activate Funeral Home")
    ctx.log("═" * 56)

    records = get_base2_onboarding_records()
    record  = pick_funeral_home(ctx, records, "Base 2 – Onboarding Funeral Homes")
    if not record:
        return

    f       = record["fields"]
    fh_name = f.get("Funeral Home Name:", "")
    ctx.info(f"Funeral Home: {fh_name}")

    # Get or ask for Interface URL
    interface_url = f.get("Interface URL", "")
    if not interface_url:
        ctx.instruction("No Interface URL found in Airtable. Please paste it now:")
        interface_url = ctx.ask("\n  🔗 Interface URL").strip()
    else:
        ctx.info(f"Interface URL: {interface_url}")

    # Save Interface URL to Airtable
    ctx.divider()
    ctx.log("\n  AUTO: Saving Interface URL to Airtable...\n")
    try:
        patch_record(_BASE2_ID(), _BASE2_TABLE_ID(), record["id"], {"Interface URL": interface_url})
        ctx.success("Interface URL saved to Base 2.")
    except Exception as e:
        ctx.warn(f"Could not save interface URL automatically: {e}")
        ctx.instruction("Please add the Interface URL to Airtable Base 2 manually.")
        ctx.verified("Done?")

    # HubSpot
    ctx.divider()
    ctx.log("\n  ADD INTERFACE LINK TO HUBSPOT\n")
    if f.get("HubSpot Record URL:"):
        ctx.link("HubSpot Record", f["HubSpot Record URL:"])
    ctx.log(f"\n     Interface URL to paste:\n     {interface_url}")
    ctx.verified("Have you added the Interface URL to HubSpot?")

    # Test Everything
    ctx.divider()
    ctx.log("\n  TEST EVERYTHING\n")
    ctx.instruction(
        "Change Email Notification and Text Notification to YOUR email "
        "and phone number in Airtable"
    )
    ctx.verified("Done?")

    ctx.instruction("Create a test contact using the interface with your own details")
    ctx.verified("Done?")

    ctx.instruction("Send a manual text using the interface you created")
    ctx.verified("Done?")

    ctx.instruction(
        'Send a text from your phone to the new SMS number asking:\n\n'
        '     "What is your address?"\n\n'
        '     Wait for the AI response.'
    )
    ctx.verified("Did you receive a response?")

    ctx.instruction("Check if the text response is accurate")
    ctx.verified("Is the response accurate?")

    ctx.instruction("Check if the email notification looks good")
    ctx.verified("Does the email look good?")

    ctx.divider()
    ctx.summary_box(f"STEP 7 SUMMARY — {fh_name}", [
        ["Funeral Home",  fh_name],
        ["Interface URL", interface_url],
        ["HubSpot",       "interface link added"],
        ["SMS test",      "sent + AI responded"],
        ["Email notif.",  "reviewed"],
        ["Status",        "ready to go live"],
    ])
    if not ctx.confirm("Does everything above look correct?"):
        ctx.warn("Fix any issues before notifying the team.")
        return

    # Slack notification
    ctx.divider()
    ctx.log("\n  NOTIFY THE TEAM\n")
    ctx.instruction("Post in Slack #aftercare-squad channel to inform the team:")
    ctx.log("\n     Suggested message:")
    ctx.log("     " + "─" * 37)
    ctx.log(f"     ✅ {fh_name} has been onboarded!")
    ctx.log(f"     Interface: {interface_url}")
    ctx.log("     " + "─" * 37)
    ctx.verified("Have you notified the team in Slack?")

    ctx.success("Step 7 complete!")


# ══════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════

def run_step(step_number: str, output_q: Queue, answer_q: Queue) -> None:
    ctx = StepContext(output_q=output_q, answer_q=answer_q)

    steps = {
        "1": step1_pick_and_sync,
        "2": step2_twilio,
        "3": step3_review_and_place_id,
        "4": step4_zapier_and_airtable,
        "5": step5_interface,
        "6": step6_qa,
        "7": step7_share_and_activate,
    }

    fn = steps.get(step_number)
    if not fn:
        raise ValueError(f"Unknown step: {step_number}")

    try:
        fn(ctx)
        output_q.put({"t": "done"})
    except InterruptedError as e:
        output_q.put({"t": "error", "m": str(e)})
    except Exception as e:
        output_q.put({"t": "error", "m": str(e)})
