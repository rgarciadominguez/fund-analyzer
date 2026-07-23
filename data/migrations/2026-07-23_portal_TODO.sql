-- ============================================================
-- PORTAL — pegar TODO esto de una vez en Supabase SQL Editor
-- (proyecto mfbtebngddjjuwfaelat). Idempotente.
-- ============================================================

-- Tabla materializada del catálogo para el webhook + lectura viva del portal (2026-07-23).
-- Pegar en Supabase → SQL Editor (proyecto fund-analyzer mfbtebngddjjuwfaelat). Idempotente.
-- La llena tools/catalog_publish.py desde catalogo_supabase.json. Cruce: ISIN.

create table if not exists public.catalogo_activos (
  isin                     text primary key,
  horfin_id                text,
  nombre                   text,
  gestora                  text,
  fund_group_id            uuid,
  es_primario_del_grupo    boolean,
  tipo_activo              text,
  geografia                text,
  categoria_rf             text,
  plazo                    text,
  estilo                   text,
  tema_sector              text,
  caracteristicas_especiales jsonb,
  srri                     smallint,
  categoria_morningstar    text,
  clasificacion_user       text,
  clasificacion_origen     text,
  opinion_user             text,
  opinion_origen           text,
  encaje_texto             text,
  filosofia                text,
  estrategia               text,
  ter_pct                  numeric,
  comision_gestion_pct     numeric,
  importe_minimo_eur       numeric,
  divisa                   text,
  distribucion             text,
  broker_disponible        jsonb,
  aum_meur                 numeric,
  anios_antiguedad         integer,
  has_qualitative_analysis boolean,
  grupo_analizado          boolean,
  fecha_ultimo_analisis    timestamptz,
  class_isins_known        jsonb,
  benchmark                text,
  estrellas                smallint,
  comision_suscripcion     boolean,
  descripcion              text,
  categoria_activo         text,
  kid                      text,
  estado                   text,
  content_hash             text,          -- para disparar webhook solo en cambios reales
  updated_at               timestamptz default now()   -- cambia SOLO cuando la fila cambia
);
create index if not exists idx_catalogo_updated_at on public.catalogo_activos (updated_at);

-- SEGURIDAD: lectura pública anon (el portal solo LEE). Escritura solo service_role (el job).
alter table public.catalogo_activos enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='catalogo_activos' and policyname='catalogo_read') then
    create policy catalogo_read on public.catalogo_activos for select using (true);
  end if;
end $$;

-- NOTA WEBHOOK: los Database Webhooks de Supabase disparan en cambios de TABLA (esta), no de
-- vistas. Configurad el webhook sobre `catalogo_activos` (INSERT/UPDATE). El payload estándar
-- trae record/old_record con la fila completa del catálogo.


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
