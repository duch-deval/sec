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
        + text[:1500]
        + "\n<|output|>\n"
    )

def _nuextract_call(model: str, schema: dict, text: str) -> Optional[dict]:
    from litellm import completion
    prompt = _nuextract_prompt(schema, text)
    try:
        resp = completion(
            model=model if model.startswith("ollama/") else f"ollama/{model}",
            messages=[{"role": "user", "content": prompt}],
            api_base="http://localhost:11434",
        )
        raw = resp.choices[0].message.content or ""
        raw = raw.split("<|output|>")[-1].split("<|end-output|>")[0].strip()
        raw = raw.strip().split("\n")[0]; return json.loads(raw)
    except Exception as e:
        logger.warning("NuExtract call failed: %s", e)
        return None


def extract_issue_size(snippet: str, model: str = DEFAULT_MODEL) -> Optional[IssueSizeExtraction]:
    """LLM fallback for issue size. Returns None on failure or raw_match mismatch."""
    if not _llm_enabled():
        return None
    if _is_nuextract(model):
        data = _nuextract_call(model, {"amount": "", "currency": "", "raw_match": ""}, snippet)
        if not data:
            return None
        try:
            amount_str = str(data.get("amount", "")).replace(",", "").replace("$", "").strip()
            raw = data.get("raw_match", "") or ""
            if not amount_str or not re.search(r"\d", amount_str):
                return None
            from .models import IssueSizeExtraction
            result = IssueSizeExtraction(amount=int(float(amount_str)), currency=data.get("currency","USD") or "USD", raw_match=raw)
            return result
        except Exception as e:
            logger.warning("NuExtract issue size parse failed: %s", e)
            return None
    time.sleep(3)
    snippet = snippet[:MAX_SNIPPET_CHARS]
    if not _llm_enabled():
        return None
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
    """LLM fallback for maturity date. Returns None on failure or raw_match mismatch."""
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
    time.sleep(3)
    snippet = snippet[:MAX_SNIPPET_CHARS]
    if not _llm_enabled():
        return None
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