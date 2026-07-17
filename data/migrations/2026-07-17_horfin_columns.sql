-- fund-analyzer — columnas nuevas para el aporte Horizonte Financiero (2026-07-17)
-- Pegar en Supabase → SQL Editor (proyecto mfbtebngddjjuwfaelat) y ejecutar.
-- Idempotente: se puede lanzar varias veces sin romper nada.

-- 1. descripcion  [BLINDADO] — QUÉ ES el fondo, factual, sin valorar (~85 chars)
alter table public.funds add column if not exists descripcion text;

-- 2. categoria_activo  [BLINDADO] — CÓMO se gestiona. Lista cerrada.
--    Eje independiente de tipo_activo (que es QUÉ es: RV/RF/Mixtos/Monetario).
alter table public.funds add column if not exists categoria_activo text;
do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'chk_categoria_activo') then
    alter table public.funds add constraint chk_categoria_activo
      check (categoria_activo is null or categoria_activo in ('Indexado','Gestionado','Hedgefund'));
  end if;
end $$;

-- 3. benchmark — categorización contra el vocabulario de Horizonte.
--    Texto libre a propósito: el clasificador puede acuñar valor nuevo con wording similar.
alter table public.funds add column if not exists benchmark text;

-- 4. kid — URL pública del KID/DFI. Debe abrir en cualquier navegador (verificado HTTP 200).
--    Nunca rutas file:///. null si no se verifica.
alter table public.funds add column if not exists kid text;

-- 5. estrellas — rating Morningstar 1-5. null si el fondo no llega a 3 años.
alter table public.funds add column if not exists estrellas smallint;
do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'chk_estrellas') then
    alter table public.funds add constraint chk_estrellas
      check (estrellas is null or (estrellas between 1 and 5));
  end if;
end $$;

-- 6. comision_suscripcion — boolean. Semántica: ¿la cobra MyInvestor? (NO el máximo del folleto)
--    null = no consta en MyInvestor.
alter table public.funds add column if not exists comision_suscripcion boolean;

-- Índice para el cruce por ISIN con Horizonte (horfin_id ya existía)
create index if not exists idx_funds_horfin_id on public.funds (horfin_id);

-- Comprobación
select column_name, data_type
from information_schema.columns
where table_schema='public' and table_name='funds'
  and column_name in ('descripcion','categoria_activo','benchmark','kid','estrellas','comision_suscripcion')
order by column_name;
