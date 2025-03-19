-- Main stored procedure to process a batch of geoIds
CREATE OR REPLACE PROCEDURE process_geoids(geoid_array text[])
LANGUAGE plpgsql
AS $$
DECLARE
    current_geoid text;
    parent_id uuid;
    place_id uuid;
    split_indices int[];
    geoid_exists boolean;
BEGIN
    -- Process each geoId in the array
    FOREACH current_geoid IN ARRAY geoid_array LOOP
        -- Start a transaction for each geoId
        BEGIN
            -- Check if place already exists
            SELECT EXISTS(SELECT 1 FROM place WHERE geoid = current_geoid) INTO geoid_exists;
            
            IF NOT geoid_exists THEN
                -- Determine split indices based on geoId length
                CASE LENGTH(current_geoid)
                    WHEN 2 THEN
                        split_indices := ARRAY[]::int[]; -- State level, no parents
                    WHEN 4 THEN
                        split_indices := ARRAY[2]; -- State + Congressional district
                    WHEN 5 THEN
                        split_indices := ARRAY[2]; -- State + County/Legislative district
                    WHEN 7 THEN
                        split_indices := ARRAY[2]; -- State + Place/City/School District
                    WHEN 10 THEN
                        split_indices := ARRAY[2, 5]; -- State + County + Subdivision
                    WHEN 11 THEN
                        split_indices := ARRAY[2, 5]; -- State + County + Census tract
                    WHEN 12 THEN
                        split_indices := ARRAY[2, 5, 11]; -- State + County + Tract + Block Group
                    ELSE
                        RAISE EXCEPTION 'Unsupported geoId length: %', LENGTH(current_geoid);
                END CASE;
                
                -- Call function to process hierarchy and get parent id
                SELECT * FROM split_geoid(split_indices, current_geoid) INTO parent_id;
                
                -- Create the new place with the full geoId
                INSERT INTO place (
                    id, 
                    created_at, 
                    updated_at, 
                    br_database_id, 
                    name, 
                    slug, 
                    geoid, 
                    mtfcc, 
                    state, 
                    upper_id
                )
                SELECT 
                    gen_random_uuid(), 
                    NOW(), 
                    NOW(), 
                    m.br_database_id, 
                    m.name, 
                    -- Generate slug from components
                    (SELECT string_agg(component, '-') FROM unnest(
                        regexp_split_to_array(current_geoid, E'(?=[0-9]{2,3}$)')
                    ) AS component),
                    current_geoid, 
                    m.mtfcc, 
                    LEFT(current_geoid, 2), 
                    parent_id
                FROM master_geoid_list m
                WHERE m.geoid = current_geoid
                RETURNING id INTO place_id;
                
                IF place_id IS NULL THEN
                    RAISE EXCEPTION 'Failed to create place for geoId: %', current_geoid;
                END IF;
            END IF;
            
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                RAISE NOTICE 'Error processing geoId %: %', current_geoid, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- Function to split a geoId and create parent places
CREATE OR REPLACE FUNCTION split_geoid(split_indices int[], geoid text)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    previous_id uuid := NULL;
    current_id uuid;
    current_geoid text;
    start_idx int := 0;
    end_idx int;
    geoid_exists boolean;
BEGIN
    -- For each split index
    FOR i IN 1..array_length(split_indices, 1) LOOP
        end_idx := split_indices[i];
        current_geoid := substring(geoid from 1 for end_idx);
        
        -- Check if this parent place exists
        SELECT EXISTS(SELECT 1 FROM place WHERE geoid = current_geoid) INTO geoid_exists;
        
        IF geoid_exists THEN
            -- Get existing place id
            SELECT id INTO current_id FROM place WHERE geoid = current_geoid;
        ELSE
            -- Create parent place
            INSERT INTO place (
                id, 
                created_at, 
                updated_at, 
                br_database_id, 
                name, 
                slug, 
                geoid, 
                mtfcc, 
                state, 
                upper_id
            )
            SELECT 
                gen_random_uuid(), 
                NOW(), 
                NOW(), 
                m.br_database_id, 
                m.name, 
                -- Generate slug
                (SELECT string_agg(component, '-') FROM unnest(
                    regexp_split_to_array(current_geoid, E'(?=[0-9]{2,3}$)')
                ) AS component),
                current_geoid, 
                m.mtfcc, 
                LEFT(current_geoid, 2), 
                previous_id
            FROM master_geoid_list m
            WHERE m.geoid = current_geoid
            RETURNING id INTO current_id;
            
            IF current_id IS NULL THEN
                RAISE EXCEPTION 'Failed to create parent place for geoId: %', current_geoid;
            END IF;
        END IF;
        
        -- Update previous_id for next iteration
        previous_id := current_id;
        start_idx := end_idx;
    END LOOP;
    
    RETURN previous_id;
END;
$$;