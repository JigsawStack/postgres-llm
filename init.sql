CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE SCHEMA IF NOT EXISTS llm;

-- Queue table for async LLM processing
CREATE TABLE IF NOT EXISTS llm.queue (
    id             bigint generated always as identity primary key,
    table_schema   text not null,
    table_name     text not null,
    row_pk         jsonb not null,
    prompt_values   hstore not null,
    prompt         text not null,
    target_columns text[] not null,
    status         text not null default 'pending',
    attempts       int not null default 0,
    last_error     text,
    created_at     timestamptz not null default now(),
    processed_at   timestamptz
);

CREATE INDEX IF NOT EXISTS queue_pending_idx
    ON llm.queue (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS queue_dedup_idx
    ON llm.queue (table_schema, table_name, row_pk) WHERE status = 'pending';

-- Trigger function: enqueues an LLM job for async processing
CREATE OR REPLACE FUNCTION llm.call()
RETURNS TRIGGER AS $$
DECLARE
    target_columns TEXT[];
    row_hstore hstore;
    prompt_vals hstore;
    pk_cols TEXT[];
    pk_jsonb JSONB;
    col_name TEXT;
    col_val TEXT;
    matches TEXT[];
BEGIN
    IF TG_ARGV[0] IS NULL THEN
        RAISE EXCEPTION 'First argument (prompt) is required';
    END IF;

    IF TG_ARGV[1] IS NULL THEN
        RAISE EXCEPTION 'Second argument (target column) is required';
    END IF;

    FOR i IN 1..TG_NARGS-1 LOOP
        target_columns := array_append(target_columns, TG_ARGV[i]);
    END LOOP;

    row_hstore := hstore(NEW);

    -- Extract only the columns referenced in the prompt as {column_name}
    prompt_vals := ''::hstore;
    SELECT array_agg(m[1]) INTO matches
    FROM regexp_matches(TG_ARGV[0], '\{(\w+)\}', 'g') AS m;

    IF matches IS NOT NULL THEN
        FOR i IN 1..array_length(matches, 1) LOOP
            col_name := matches[i];
            IF NOT row_hstore ? col_name THEN
                RAISE EXCEPTION 'Column {%} referenced in prompt does not exist in table %.%', col_name, TG_TABLE_SCHEMA, TG_TABLE_NAME;
            END IF;
            prompt_vals := prompt_vals || hstore(col_name, row_hstore -> col_name);
        END LOOP;
    END IF;

    SELECT array_agg(a.attname ORDER BY a.attnum)
    INTO pk_cols
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = TG_RELID AND i.indisprimary;

    IF pk_cols IS NULL THEN
        RAISE EXCEPTION 'Table %.% has no primary key — llm.call requires one', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;

    pk_jsonb := '{}'::jsonb;
    FOR i IN 1..array_length(pk_cols, 1) LOOP
        col_name := pk_cols[i];
        col_val := row_hstore -> col_name;
        pk_jsonb := pk_jsonb || jsonb_build_object(col_name, col_val);
    END LOOP;

    UPDATE llm.queue
    SET prompt_values = prompt_vals, created_at = now()
    WHERE table_schema = TG_TABLE_SCHEMA
      AND table_name = TG_TABLE_NAME
      AND row_pk = pk_jsonb
      AND status = 'pending';

    IF NOT FOUND THEN
        INSERT INTO llm.queue (table_schema, table_name, row_pk, prompt_values, prompt, target_columns)
        VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, pk_jsonb, prompt_vals, TG_ARGV[0], target_columns);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Worker function: processes pending LLM jobs in batches
CREATE OR REPLACE FUNCTION llm.process_queue(batch_size INT DEFAULT 10)
RETURNS INT AS $$
DECLARE
    -- Customizable variables or can be referenced from environment variables/Vault
    API_KEY CONSTANT TEXT := '<your-api-key>'; -- Get your api key from https://interfaze.ai/dashboard
    BASE_URL CONSTANT TEXT := 'https://api.interfaze.ai/v1/chat/completions';
    MODEL_NAME CONSTANT TEXT := 'interfaze-beta';
    MAX_TOKENS CONSTANT INTEGER := 1000;
    MAX_ATTEMPTS CONSTANT INTEGER := 3;

    job RECORD;
    prompt_text TEXT;
    col_keys TEXT[];
    col_name TEXT;
    col_value TEXT;
    target_columns TEXT[];
    schema_properties JSONB;
    response_format JSON;
    request_body TEXT;
    response RECORD;
    llm_result TEXT;
    llm_json JSONB;
    update_sql TEXT;
    where_clause TEXT;
    pk_key TEXT;
    pk_value TEXT;
    processed INT := 0;
BEGIN
    IF API_KEY IS NULL OR length(API_KEY) < 15 THEN
        RAISE EXCEPTION 'API_KEY not configured. Edit llm.process_queue() and replace <your-api-key> with your actual API key.';
    END IF;

    SET http.timeout_msec = 30000;

    FOR job IN
        SELECT * FROM llm.queue
        WHERE status = 'pending'
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT batch_size
    LOOP
        BEGIN
            UPDATE llm.queue SET status = 'running', attempts = attempts + 1 WHERE id = job.id;

            prompt_text := job.prompt;
            target_columns := job.target_columns;
            col_keys := akeys(job.prompt_values);

            FOR i IN 1..array_length(col_keys, 1) LOOP
                col_name := col_keys[i];
                col_value := job.prompt_values -> col_name;
                prompt_text := replace(prompt_text, '{' || col_name || '}', coalesce(col_value, ''));
            END LOOP;

            IF prompt_text IS NULL OR prompt_text = '' THEN
                RAISE EXCEPTION 'Prompt text cannot be null or empty';
            END IF;

            prompt_text := prompt_text || E'\n\nReturn a JSON object with exactly these keys: '
                || array_to_string(target_columns, ', ');

            schema_properties := '{}'::jsonb;
            FOR i IN 1..array_length(target_columns, 1) LOOP
                schema_properties := schema_properties || jsonb_build_object(
                    target_columns[i], jsonb_build_object('type', 'string')
                );
            END LOOP;

            response_format := json_build_object(
                'type', 'json_schema',
                'json_schema', json_build_object(
                    'name', 'column_values',
                    'strict', true,
                    'schema', json_build_object(
                        'type', 'object',
                        'properties', schema_properties::json,
                        'required', to_json(target_columns),
                        'additionalProperties', false
                    )
                )
            );

            request_body := json_build_object(
                'model', MODEL_NAME,
                'messages', json_build_array(
                    json_build_object(
                        'role', 'user',
                        'content', prompt_text
                    )
                ),
                'max_tokens', MAX_TOKENS,
                'response_format', response_format
            )::TEXT;

            SELECT * INTO response
            FROM http((
                'POST',
                BASE_URL,
                ARRAY[
                    http_header('Content-Type', 'application/json'),
                    http_header('Authorization', 'Bearer ' || API_KEY)
                ],
                'application/json',
                request_body
            )::http_request);

            IF response.status != 200 THEN
                RAISE EXCEPTION 'API request failed with status %: %', response.status, response.content;
            END IF;

            llm_result := (response.content::jsonb)->'choices'->0->'message'->>'content';
            llm_json := llm_result::jsonb;

            update_sql := 'UPDATE ' || quote_ident(job.table_schema) || '.' || quote_ident(job.table_name) || ' SET ';

            FOR i IN 1..array_length(target_columns, 1) LOOP
                IF i > 1 THEN
                    update_sql := update_sql || ', ';
                END IF;
                update_sql := update_sql || quote_ident(target_columns[i]) || ' = ' || quote_literal(llm_json ->> target_columns[i]);
            END LOOP;

            where_clause := '';
            FOR pk_key, pk_value IN SELECT * FROM jsonb_each_text(job.row_pk)
            LOOP
                IF where_clause != '' THEN
                    where_clause := where_clause || ' AND ';
                END IF;
                where_clause := where_clause || quote_ident(pk_key) || ' = ' || quote_literal(pk_value);
            END LOOP;

            update_sql := update_sql || ' WHERE ' || where_clause;
            EXECUTE update_sql;

            UPDATE llm.queue SET status = 'done' WHERE id = job.id;
            processed := processed + 1;

        EXCEPTION
            WHEN OTHERS THEN
                IF job.attempts + 1 >= MAX_ATTEMPTS THEN
                    UPDATE llm.queue SET status = 'error', last_error = SQLERRM, processed_at = now() WHERE id = job.id;
                ELSE
                    UPDATE llm.queue SET status = 'pending', last_error = SQLERRM WHERE id = job.id;
                END IF;
        END;
    END LOOP;

    DELETE FROM llm.queue WHERE status = 'done';

    RETURN processed;
END;
$$ LANGUAGE plpgsql;

-- Schedule the worker to process pending jobs every 5 seconds
SELECT cron.schedule('llm-worker', '5 seconds', $$SELECT llm.process_queue(20)$$);
