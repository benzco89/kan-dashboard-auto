# -*- coding: utf-8 -*-
"""Which Gemini models exist, and what does a classify call really cost?

READ ONLY. Writes to no sheet, uploads nothing, archives nothing.

Two questions the archive's cost estimate rests on and neither was measured:

  1. **Which flash model ids are actually served to this key.** `media_archiver`
     pins `gemini-3.5-flash` with a fallback to `gemini-2.5-pro`. Guessing a
     newer id and getting it wrong is not a harmless typo: every call would
     fail and fall through to Pro, which is the expensive path. So enumerate
     rather than assume.
  2. **What thinking costs.** Thinking tokens bill as output. An estimate built
     from the length of the returned JSON (~95 tokens) ignores them entirely
     and can be wrong by an order of magnitude. This runs the real archive
     prompt twice per model - once with default thinking, once with
     thinking_budget=0 - and prints usage_metadata for both.

    gh workflow run probe_gemini_models.yml     # GEMINI_API_KEY lives in CI

Env: GEMINI_API_KEY (required), PROBE_MODELS (optional, comma-separated ids to
     test; default: every served flash model plus the pinned pair).
"""

import os

from google import genai
from google.genai import types

import media_archiver as ma

KEY = os.environ.get("GEMINI_API_KEY")

# The real prompt shape, with a caption typical of the index (median 154 chars).
CAPTION = ("המרוץ נגד הזמן בנפאל: 10 ימים לאחר השיטפונות הקטלניים שפקדו את "
           "המדינה, צוותי החילוץ ממשיכים לחפש ניצולים בין ההריסות. למעלה מ-200 "
           "בני אדם נספו (יחזקאל קורנברג)")


def build_prompt():
    cats = " · ".join(ma.TOPIC_CATEGORIES)
    return f"""אתה עורך ארכיון של חדר חדשות (כאן חדשות). לפניך כיתוב של סרטון
שפורסם באינסטגרם.

לא זוהתה תוכנית.

הכיתוב:
{CAPTION}

החזר קטגוריה אחת מתוך: {cats}
ותגיות חופשיות שמזהות את **האירוע או הסיפור הספציפי** (למשל "בחירות 2026",
"חטיפת יהלי") - לא מילות מפתח כלליות. אם הכיתוב לא מספיק כדי לזהות סיפור,
החזר תגיות ריקות ואל תמציא.
summary: שורה אחת בעברית שמתארת מה רואים בסרטון."""


def list_models(client):
    served = []
    for m in client.models.list():
        name = (m.name or "").replace("models/", "")
        actions = getattr(m, "supported_actions", None) or []
        if "generateContent" in actions or not actions:
            served.append(name)
    return served


def run_once(client, model, prompt, budget):
    cfg = {"response_mime_type": "application/json",
           "response_schema": ma.TOPIC_SCHEMA}
    if budget is not None:
        cfg["thinking_config"] = types.ThinkingConfig(thinking_budget=budget)
    res = client.models.generate_content(
        model=model, contents=prompt,
        config=types.GenerateContentConfig(**cfg))
    u = res.usage_metadata
    return {
        "in": getattr(u, "prompt_token_count", 0) or 0,
        "out": getattr(u, "candidates_token_count", 0) or 0,
        "think": getattr(u, "thoughts_token_count", 0) or 0,
        "total": getattr(u, "total_token_count", 0) or 0,
        "text": (res.text or "")[:120],
    }


def main():
    if not KEY:
        raise SystemExit("GEMINI_API_KEY is missing")
    client = genai.Client(api_key=KEY)

    print("=" * 70)
    print("Gemini model + thinking-cost probe · READ ONLY")
    print("=" * 70)

    served = list_models(client)
    flash = sorted(n for n in served if "flash" in n)
    print(f"\n{len(served)} models served to this key. flash ids:")
    for n in flash:
        print(f"   {n}")

    wanted = os.environ.get("PROBE_MODELS")
    targets = ([w.strip() for w in wanted.split(",") if w.strip()] if wanted
               else sorted(set(flash) | set(ma.GEMINI_MODELS)))

    prompt = build_prompt()
    print(f"\nprompt: {len(prompt)} chars")
    print("\n" + "=" * 70)
    print(f"{'model':34s} {'mode':9s} {'in':>6s} {'think':>6s} {'out':>6s}")
    print("=" * 70)
    for model in targets:
        for label, budget in (("default", None), ("budget=0", 0)):
            try:
                r = run_once(client, model, prompt, budget)
            except Exception as e:
                print(f"{model:34s} {label:9s} ❌ {str(e)[:60]}")
                continue
            print(f"{model:34s} {label:9s} {r['in']:6d} {r['think']:6d} "
                  f"{r['out']:6d}   {r['text'][:52]}")


if __name__ == "__main__":
    main()
