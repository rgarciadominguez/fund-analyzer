-- 2026-09-23 — funds.clasificacion_user admite 'Malo'
-- Por qué: el portal Horizonte clasifica Top / Bueno / Medio / Malo (20 fondos en 'Malo'). El check
-- chk_clasificacion_user solo admitía Top/Bueno/Medio/Clase_similar/Clase_sucia, así que el consumer
-- portal→Supabase (tools/consume_inputs_rafa) fallaba en esas filas con un WARN y esos fondos se
-- quedaban sin clasificación en el catálogo. Aplicada vía Supabase MCP (apply_migration).
ALTER TABLE public.funds DROP CONSTRAINT IF EXISTS chk_clasificacion_user;
ALTER TABLE public.funds ADD CONSTRAINT chk_clasificacion_user
  CHECK (clasificacion_user IS NULL OR clasificacion_user = ANY (ARRAY['Top','Bueno','Medio','Malo','Clase_similar','Clase_sucia']));
