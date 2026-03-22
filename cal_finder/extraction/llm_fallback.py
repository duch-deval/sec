from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

import instructor
from litellm import completion

from .models import IssueSizeExtraction, MaturityDateExtraction, BDReferenceExtraction

logger = logging.getLogger(__name__)

# Snippet max length — never send full documents to LLM
MAX_SNIPPET_CHARS = 1500

# Model used for prototyping — swap to NuExtract in Week 8
DEFAULT_MODEL = "gpt-4o-mini"


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


def extract_maturity_date(snippet: str, model: str = DEFAULT_MODEL) -> Optional[MaturityDateExtraction]:
    """
    LLM fallback for maturity date. Returns None on failure or raw_match mismatch.
    Only called when regex returns a year-only value or empty.
    """
    snippet = snippet[:MAX_SNIPPET_CHARS]
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
        logger.warning("Maturity date LLM fallback failed: %s", e)
        return None


def extract_bd_by_reference(snippet: str, model: str = DEFAULT_MODEL) -> Optional[BDReferenceExtraction]:
    """
    LLM fallback for BD-by-reference. Returns None on failure or raw_match mismatch.
    Only called when BD definition defers to 'Legal Holiday' or 'Place of Payment'.
    """
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
