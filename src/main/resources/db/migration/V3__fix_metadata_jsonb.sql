-- Fix metadata column type to jsonb for better indexing and alignment with Hibernate types
ALTER TABLE nodes ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
