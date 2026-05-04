"""Constantes centralizadas de modelos LLM.

Cambiar el modelo de un agente concreto desde aquí. Evita magic strings
dispersos y facilita migraciones (ej. cuando salga Haiku 4.6 o Opus 4.8).

Convención (Fase M, 2026-05-04):
- HAIKU_HINTS: tareas de clasificación pura, hints de dominios, JSON
  estructurado simple. Haiku 4.5 basta. ~5x más barato que Opus.
- OPUS_AUDIT: tareas que requieren reasoning profundo + entendimiento
  semántico cruzando múltiples secciones. Justifica el coste.
- SONNET_FALLBACK: escalation cuando Haiku da confidence=low en
  decisiones críticas (ej. lead/co manager con varios candidatos).
- SONNET_DEFAULT: T1 del analyst para síntesis narrativa.
"""

# Tier 0 — clasificación / hints (5x más barato que Opus)
HAIKU_HINTS = "claude-haiku-4-5-20251001"

# Tier 1 — síntesis narrativa default (analyst)
SONNET_DEFAULT = "claude-sonnet-4-5"

# Tier 1 — escalation desde Haiku cuando confidence baja
SONNET_FALLBACK = "claude-sonnet-4-5"

# Tier 2 — audit semántico profundo (analyst _opus_*)
OPUS_AUDIT = "claude-opus-4-20250514"

# Gemini (cuando aplica)
GEMINI_FLASH = "gemini-2.5-flash"
GEMINI_PRO = "gemini-2.5-pro"
