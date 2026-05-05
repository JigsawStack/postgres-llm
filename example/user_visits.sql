create table public.user_visits (
  id uuid not null default gen_random_uuid (),
  created_at timestamp with time zone not null default now(),
  full_name text not null,
  company text null,
  summary text null,
  linkedin text null,
  constraint user_visits_pkey primary key (id)
) TABLESPACE pg_default;

create trigger user_visits_search_summary
after INSERT
or
update OF full_name on user_visits for EACH row when (new.full_name is not null)
execute FUNCTION llm.call (
  'Give a summary background on who this person and their linkedin url. Details: {full_name}, {company}',
  'summary',
  'linkedin'
);