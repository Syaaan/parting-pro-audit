-- Zap Audit log table. Run this once in the Supabase SQL editor
-- (Project → SQL Editor → New query) for project ykayxonkdzgvzrqktcyt.

create table if not exists public.zap_runs (
  id            bigint generated always as identity primary key,
  run_id        text unique,
  zap_name      text,
  zap_id        text,
  status        text,
  ts            timestamptz not null,
  step          text,
  error_message text,
  duration_ms   integer,
  task_count    integer,
  logger_source text,
  created_at    timestamptz not null default now()
);

create index if not exists zap_runs_ts_idx on public.zap_runs (ts desc);
create index if not exists zap_runs_status_idx on public.zap_runs (status);

alter table public.zap_runs enable row level security;

-- Dashboard (anon key) can only read. Inserts come from the Zapier Logger zap
-- using the service_role key, which bypasses RLS automatically — no insert
-- policy needed, and none should be added for anon/authenticated.
create policy "zap_runs_select_anon" on public.zap_runs
  for select
  to anon
  using (true);
