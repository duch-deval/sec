from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

import time
import instructor
from litellm import completion

from .models import IssueSizeExtraction, MaturityDateExtraction, BDReferenceExtraction

logger = logging.getLogger(__name__)

def _llm_enabled() -> bool:
    import os
    return os.environ.get("LLM_FALLBACK_ENABLED", "true").lower() == "true" 

# Snippet max length — never send full documents to LLM
MAX_SNIPPET_CHARS = 1500

# Model used for prototyping — swap to NuExtract in Week 8
DEFAULT_MODEL = "claude-haiku-4-5-20251001"


def _load_prompt(field_name: str) -> str:
    """Load prompt from prompts/<field_name>.md, strip markdown fences."""
    prompt_path = Path(__file__).parent / "prompts" / f"{field_name}.md"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    text = prompt_path.read_text(encoding="utf-8")
    # Strip markdown code fences if present
    lines = text.splitlines()
    lines = [l for l in lines if not l.strip().startswith("```")]
    return "\n".join(lines).strip()


def _make_client():
    """Patch litellm completion with instructor for Pydantic enforcement."""
    return instructor.from_litellm(completion)


def extract_issue_size(snippet: str, model: str = DEFAULT_MODEL) -> Optional[IssueSizeExtraction]:
    """
    LLM fallback for issue size. Returns None on failure or raw_match mismatch.
    Only called when regex returns empty.
    """
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
    """Remove indenture execution date lines that confuse LLM into returning wrong date."""
    import re
    # Remove "Dated as of Month DD, YYYY" lines — these are execution dates, not maturity dates
    snippet = re.sub(r'[Dd]ated\s+as\s+of\s+\w+\s+\d{1,2},?\s+20\d{2}', '', snippet)
    # Remove "dated Month DD, YYYY" variants
    snippet = re.sub(r'dated[^.]{0,40}20\d{2}', '', snippet, flags=re.IGNORECASE)
    return snippet.strip()


def extract_maturity_date(snippet: str, model: str = DEFAULT_MODEL) -> Optional[MaturityDateExtraction]:
    """
    LLM fallback for maturity date. Returns None on failure or raw_match mismatch.
    Only called when regex returns a year-only value or empty.
    """
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
                import json as _json
                _calls = e.__context__.choices[0].message.tool_calls
                result = MaturityDateExtraction(**_json.loads(_calls[0].function.arguments))
                if result.verify_raw_match(snippet):
                    return result
            except Exception:
                pass
        logger.warning("Maturity date LLM fallback failed: %s", e)
        return None


def extract_bd_by_reference(snippet: str, model: str = DEFAULT_MODEL) -> Optional[BDReferenceExtraction]:
    """
    LLM fallback for BD-by-reference. Returns None on failure or raw_match mismatch.
    Only called when BD definition defers to 'Legal Holiday' or 'Place of Payment'.
    """
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
