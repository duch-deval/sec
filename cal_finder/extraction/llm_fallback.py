from __future__ import annotations
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Optional

import instructor
from litellm import completion

from .models import IssueSizeExtraction, MaturityDateExtraction, BDReferenceExtraction

logger = logging.getLogger(__name__)

MAX_SNIPPET_CHARS = 1500
DEFAULT_MODEL = os.environ.get("LLM_MODEL", "claude-haiku-4-5-20251001")


def _llm_enabled() -> bool:
    return os.environ.get("LLM_FALLBACK_ENABLED", "true").lower() == "true"


def _load_prompt(field_name: str) -> str:
    """Load prompt from prompts/<field_name>.md, strip markdown fences."""
    prompt_path = Path(__file__).parent / "prompts" / f"{field_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    text = prompt_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    lines = [l for l in lines if not l.strip().startswith("```")]
    return "\n".join(lines).strip()


def _make_client():
    """Patch litellm completion with instructor for Pydantic enforcement."""
    return instructor.from_litellm(completion)


_NUEXTRACT_MODELS = {"nuextract"}

def _is_nuextract(model: str) -> bool:
    return any(m in model.lower() for m in _NUEXTRACT_MODELS)

def _nuextract_prompt(schema: dict, text: str) -> str:
    return (
        "<|input|>\n### Template:\n"
        + json.dumps(schema)
        + "\n### Text:\n"
        + text[:MAX_SNIPPET_CHARS]
        + "\n<|output|>\n"
    )

def _nuextract_call(model: str, schema: dict, text: str) -> Optional[dict]:
    """Call NuExtract via Ollama, parse JSON output. Returns None on any failure."""
    prompt = _nuextract_prompt(schema, text)
    raw = ""
    try:
        resp = completion(
            model=model if model.startswith("ollama/") else f"ollama/{model}",
            messages=[{"role": "user", "content": prompt}],
            api_base="http://localhost:11434",
        )
        raw = resp.choices[0].message.content or ""
        # Strip NuExtract wrapper tokens — leaves a full pretty-printed JSON block
        raw = raw.split("<|output|>")[-1].split("<|end-output|>")[0].strip()
        # Extract first complete JSON object only — ignore any trailing model commentary
        brace = raw.find("{")
        if brace != -1:
            depth, end = 0, -1
            for i, ch in enumerate(raw[brace:], brace):
                if ch == "{": depth += 1
                elif ch == "}": depth -= 1
                if depth == 0: end = i + 1; break
            raw = raw[brace:end] if end != -1 else raw
        # Guard: if output contains ### it's a context bleed — model returned its own schema
        if "### " in raw:
            logger.warning("NuExtract context bleed detected — discarding")
            return None
        data = json.loads(raw)
        # Guard: reject if keys don't match expected schema keys
        if not any(k in data for k in schema):
            logger.warning("NuExtract returned wrong schema — discarding")
            return None
        return data
    except json.JSONDecodeError:
        try:
            repaired = re.sub(r'(\{|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1 "\2":', raw)
            return json.loads(repaired)
        except Exception:
            logger.warning("NuExtract call failed: could not parse JSON: %r", raw[:80])
            return None
    except Exception as e:
        logger.warning("NuExtract call failed: %s", e)
        return None


def extract_issue_size(snippet: str, model: str = DEFAULT_MODEL) -> Optional[IssueSizeExtraction]:
    """LLM fallback for issue size. Returns None on failure or raw_match mismatch."""
    if not _llm_enabled():
        return None
    if _is_nuextract(model):
        time.sleep(1)
        data = _nuextract_call(model, {"amount": "", "currency": "", "raw_match": ""}, snippet)
        if not data:
            return None
        try:
            amount_str = str(data.get("amount", "")).replace(",", "").replace("$", "").strip()
            # Guard: reject coupon rates mistaken for issue size
            if "%" in amount_str:
                logger.warning("NuExtract issue size parse failed: looks like coupon rate: %r", amount_str)
                return None
            if not amount_str or not re.search(r"\d", amount_str):
                return None
            amount = int(float(amount_str))
            # Guard: minimum plausible issue size
            if amount < 100_000:
                logger.warning("NuExtract issue size parse failed: amount %d too small", amount)
                return None
            # NuExtract raw_match is unreliable — verify amount appears in snippet instead
            if not re.search(re.escape(amount_str.split(".")[0][-6:]), snippet):
                logger.warning("NuExtract issue size: amount %r not found in snippet — discarding", amount_str)
                return None
            result = IssueSizeExtraction(
                amount=amount,
                currency=data.get("currency", "USD") or "USD",
                raw_match=amount_str,  # use amount_str as synthetic raw_match
            )
            return result
        except Exception as e:
            logger.warning("NuExtract issue size parse failed: %s", e)
            return None
    # Haiku path
    time.sleep(3)
    snippet = snippet[:MAX_SNIPPET_CHARS]
    try:
        prompt = _load_prompt("issue_size")
        client = _make_client()
        result = client.chat.completions.create(
            model=model,
            temperature=0.0,
            response_model=IssueSizeExtraction,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Extract the issue size from this text:\n\n{snippet}"},
            ],
        )
        if not result.verify_raw_match(snippet):
            logger.warning("Issue size raw_match failed verification — discarding")
            return None
        return result
    except Exception as e:
        logger.warning("Issue size LLM fallback failed: %s", e)
        return None


def _clean_maturity_snippet(snippet: str) -> str:
    """Strip indenture execution date lines to prevent LLM returning the wrong date."""
    snippet = re.sub(r'[Dd]ated\s+as\s+of\s+\w+\s+\d{1,2},?\s+20\d{2}', '', snippet)
    snippet = re.sub(r'dated[^.]{0,40}20\d{2}', '', snippet, flags=re.IGNORECASE)
    return snippet.strip()


def extract_maturity_date(snippet: str, model: str = DEFAULT_MODEL) -> Optional[MaturityDateExtraction]:
    """LLM fallback for maturity date. Gate removed April 2 — 100% regex now.
    Retained for Haiku compatibility but not called in production."""
    time.sleep(3)
    snippet = _clean_maturity_snippet(snippet[:MAX_SNIPPET_CHARS])
    if not _llm_enabled():
        return None
    try:
        prompt = _load_prompt("maturity_date")
        client = _make_client()
        result = client.chat.completions.create(
            model=model,
            temperature=0.0,
            response_model=MaturityDateExtraction,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Extract the maturity date from this text:\n\n{snippet}"},
            ],
        )
        if not result.verify_raw_match(snippet):
            logger.warning("Maturity date raw_match failed verification — discarding")
            return None
        return result
    except Exception as e:
        if "does not support multiple tool calls" in str(e):
            try:
                _calls = e.__context__.choices[0].message.tool_calls
                result = MaturityDateExtraction(**json.loads(_calls[0].function.arguments))
                if result.verify_raw_match(snippet):
                    return result
            except Exception:
                pass
        logger.warning("Maturity date LLM fallback failed: %s", e)
        return None


def extract_bd_by_reference(snippet: str, model: str = DEFAULT_MODEL) -> Optional[BDReferenceExtraction]:
    """LLM fallback for BD-by-reference. Returns None on failure or raw_match mismatch."""
    if not _llm_enabled():
        return None
    if _is_nuextract(model):
        time.sleep(1)
        data = _nuextract_call(model, {"locations": [], "raw_match": ""}, snippet)
        if not data:
            return None
        try:
            result = BDReferenceExtraction(
                locations=data.get("locations") or [],
                raw_match=data.get("raw_match", "") or "",
            )
            if not result.verify_raw_match(snippet):
                logger.warning("BD reference raw_match failed verification — discarding")
                return None
            return result
        except Exception as e:
            logger.warning("NuExtract BD reference parse failed: %s", e)
            return None
    # Haiku path
    time.sleep(3)
    snippet = snippet[:MAX_SNIPPET_CHARS]
    try:
        prompt = _load_prompt("payment_calendar")
        client = _make_client()
        result = client.chat.completions.create(
            model=model,
            temperature=0.0,
            response_model=BDReferenceExtraction,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Extract the business day locations from this text:\n\n{snippet}"},
            ],
        )
        if not result.verify_raw_match(snippet):
            logger.warning("BD reference raw_match failed verification — discarding")
            return None
        return result
    except Exception as e:
        logger.warning("BD reference LLM fallback failed: %s", e)
        return None
