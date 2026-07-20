-- Tablas de coste para el panel admin en la web PÚBLICA (2026-07-20).
-- Pegar en Supabase → SQL Editor (proyecto fund-analyzer mfbtebngddjjuwfaelat) y ejecutar.
-- Idempotente.

-- Coste de por vida por fondo (nivel ANÁLISIS DE FONDO)
create table if not exists public.cost_fund (
  isin              text primary key,
  cost_usd          numeric,
  cost_analisis_usd numeric,   -- análisis de fondos (texto)
  cost_imagenes_usd numeric,   -- procesar imágenes (visión)
  n_calls           integer,
  updated_at        timestamptz default now()
);

-- Coste por mes natural y categoría (nivel MENSUAL)
create table if not exists public.cost_month (
  mes         text,            -- YYYY-MM
  categoria   text,            -- analisis_fondos | procesar_imagenes | otros
  cost_usd    numeric,
  n_calls     integer,
  updated_at  timestamptz default now(),
  primary key (mes, categoria)
);

-- Lectura pública (la web anónima lee estos agregados; no hay datos sensibles).
alter table public.cost_fund  enable row level security;
alter table public.cost_month enable row level security;
do $$ begin
  if not exists (select 1 from pg_policies where tablename='cost_fund' and policyname='cost_fund_read') then
    create policy cost_fund_read on public.cost_fund for select using (true);
  end if;
  if not exists (select 1 from pg_policies where tablename='cost_month' and policyname='cost_month_read') then
    create policy cost_month_read on public.cost_month for select using (true);
  end if;
end $$;
