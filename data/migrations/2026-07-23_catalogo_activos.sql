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
