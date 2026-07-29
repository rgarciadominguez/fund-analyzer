-- Añade columnas underwater + n_puntos a hf_asset_metrics (2026-07-23, Bug A/B).
-- Pegar en Supabase → SQL Editor. Idempotente.
alter table public.hf_asset_metrics add column if not exists dias_bajo_agua integer;
alter table public.hf_asset_metrics add column if not exists racha_max_bajo_agua_dias integer;
alter table public.hf_asset_metrics add column if not exists pct_tiempo_bajo_agua numeric;
alter table public.hf_asset_metrics add column if not exists n_puntos integer;
