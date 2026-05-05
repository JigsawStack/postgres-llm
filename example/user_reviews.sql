create table public.user_reviews (
  user_id uuid not null default gen_random_uuid (),
  created_at timestamp with time zone not null default now(),
  review_text text not null,
  sentiment text null,
  translation text null,
  constraint user_reviews_pkey primary key (user_id)
) TABLESPACE pg_default;

create trigger analyze_sentiment
after INSERT
or
update OF review_text on user_reviews for EACH row when (new.review_text is not null)
execute FUNCTION llm.call (
  'Analyze the sentiment of this text and respond with only "positive", "negative", or "neutral". return value in lowercase. Also translate the text to english and if the text is already in english, keep it the same. Text: {review_text}',
  'sentiment',
  'translation'
);