CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE OR REPLACE FUNCTION call_llm()
RETURNS TRIGGER AS $$
DECLARE
    -- Customizable variables or can be referenced from environment variables/Vault
    API_KEY CONSTANT TEXT := '<your-api-key>'; -- Get your api key from https://interfaze.ai/dashboard
    BASE_URL CONSTANT TEXT := 'https://api.interfaze.ai/v1/chat/completions';
    MODEL_NAME CONSTANT TEXT := 'interfaze-alpha';
    MAX_TOKENS CONSTANT INTEGER := 1000;
    
    prompt_text TEXT;
    target_column TEXT;
    context_column TEXT;
    context_value TEXT;
    request_body TEXT;
    response RECORD;
    llm_result TEXT;
BEGIN
    IF TG_ARGV[1] IS NULL THEN
        RAISE EXCEPTION 'Second argument (target column) is required';
    END IF;
    target_column := TG_ARGV[1];

    IF TG_ARGV[0] IS NULL THEN
        RAISE EXCEPTION 'First argument (prompt) is required';
    END IF;
    
    prompt_text := TG_ARGV[0];

    IF TG_ARGV[2] IS NOT NULL THEN
        context_column := TG_ARGV[2];
        
        BEGIN
            context_value := (hstore(NEW) -> context_column);
            
            IF context_value IS NOT NULL AND context_value != '' THEN
                prompt_text := prompt_text || ' Context: "' || context_value || '"';
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to read context column %: %', context_column, SQLERRM;
        END;
    END IF;

    IF prompt_text IS NULL OR prompt_text = '' THEN
        RAISE EXCEPTION 'Prompt text cannot be null or empty';
    END IF;

    request_body := json_build_object(
        'model', MODEL_NAME,
        'messages', json_build_array(
            json_build_object(
                'role', 'user',
                'content', prompt_text
            )
        ),
        'max_tokens', MAX_TOKENS
    )::TEXT;

    SET http.curlopt_timeout_msec = 30000;
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
        NEW := NEW #= hstore(target_column, llm_result);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to update column %: %', target_column, SQLERRM;
    END;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
set statement_timeout TO '1min';