-- Satélites de quant POR ISIN para la conexión viva del portal (2026-07-23).
-- Pegar en Supabase → SQL Editor (proyecto fund-analyzer mfbtebngddjjuwfaelat). Idempotente.
-- Las llena tools/quant_sync.py aplanando el JSONB de fund_groups a por-ISIN.

-- Rendimientos por año natural (una fila por ISIN y año)
create table if not exists public.hf_asset_annual_returns (
  isin        text not null,
  anio        integer not null,
  rentab_pct  numeric,             -- % del año natural
  updated_at  timestamptz default now(),
  primary key (isin, anio)
);

-- Métricas de riesgo/rentabilidad (una fila por ISIN)
create table if not exists public.hf_asset_metrics (
  isin              text primary key,
  cagr_desde_inicio numeric,
  rentab_1a         numeric,
  rentab_3a         numeric,       -- anualizada
  rentab_5a         numeric,
  rentab_10a        numeric,
  volatilidad       numeric,       -- desde inicio (mensual anualizada)
  volatilidad_3a    numeric,
  volatilidad_5a    numeric,
  max_drawdown      numeric,       -- negativo
  peor_anio         numeric,
  mejor_anio        numeric,
  estrellas         smallint,      -- Morningstar 1-5
  medalist          text,          -- Gold/Silver/Bronze/Neutral/Negative
  srri              smallint,      -- 1-7
  mstar_rating      smallint,
  fuente            text,
  updated_at        timestamptz default now()
);

-- Serie diaria de precios/NAV (una fila por ISIN y fecha) — la llena --prices
create table if not exists public.hf_asset_prices (
  isin        text not null,
  fecha       date not null,
  nav         numeric,
  primary key (isin, fecha)
);
create index if not exists idx_hf_prices_isin on public.hf_asset_prices (isin);

-- Lectura pública (agregados de mercado, sin datos sensibles)
alter table public.hf_asset_annual_returns enable row level security;
alter table public.hf_asset_metrics        enable row level security;
alter table public.hf_asset_prices         enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='hf_asset_annual_returns' and policyname='hf_ar_read') then
    create policy hf_ar_read on public.hf_asset_annual_returns for select using (true); end if;
  if not exists (select 1 from pg_policies where tablename='hf_asset_metrics' and policyname='hf_met_read') then
    create policy hf_met_read on public.hf_asset_metrics for select using (true); end if;
  if not exists (select 1 from pg_policies where tablename='hf_asset_prices' and policyname='hf_pr_read') then
    create policy hf_pr_read on public.hf_asset_prices for select using (true); end if;
end $$;
