CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE OR REPLACE FUNCTION call_llm()
RETURNS TRIGGER AS $$
DECLARE
    -- Customizable variables or can be referenced from environment variables/Vault
    API_KEY CONSTANT TEXT := '<your-api-key>'; -- Get your api key from https://interfaze.ai/dashboard
    BASE_URL CONSTANT TEXT := 'https://api.interfaze.ai/v1/chat/completions';
    MODEL_NAME CONSTANT TEXT := 'interfaze-beta';
    MAX_TOKENS CONSTANT INTEGER := 1000;
    
    prompt_text TEXT;
    target_columns TEXT[];
    request_body TEXT;
    response RECORD;
    llm_result TEXT;
    llm_json JSONB;
    col_name TEXT;
    col_value TEXT;
    row_hstore hstore;
    placeholder TEXT;
    col_keys TEXT[];
    schema_properties JSONB;
    response_format JSON;
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
    
    prompt_text := TG_ARGV[0];
    row_hstore := hstore(NEW);
    col_keys := akeys(row_hstore);

    FOR i IN 1..array_length(col_keys, 1) LOOP
        col_name := col_keys[i];
        placeholder := '{' || col_name || '}';
        
        IF position(placeholder IN prompt_text) > 0 THEN
            col_value := row_hstore -> col_name;
            IF col_value IS NOT NULL THEN
                prompt_text := replace(prompt_text, placeholder, col_value);
            ELSE
                prompt_text := replace(prompt_text, placeholder, '');
            END IF;
        END IF;
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

    SET http.timeout_msec = 30000;
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
        RAISE WARNING 'API request failed with status %: %', response.status, response.content;
        RETURN NEW;
    END IF;

    BEGIN
        llm_result := (response.content::jsonb)->'choices'->0->'message'->>'content';
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to parse response: %', response.content;
            RETURN NEW;
    END;

    BEGIN
        llm_json := llm_result::jsonb;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'LLM returned invalid JSON: %', llm_result;
            RETURN NEW;
    END;

    BEGIN
        FOR i IN 1..array_length(target_columns, 1) LOOP
            col_value := llm_json ->> target_columns[i];
            NEW := NEW #= hstore(target_columns[i], col_value);
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to update columns: %', SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
set statement_timeout TO '1min';