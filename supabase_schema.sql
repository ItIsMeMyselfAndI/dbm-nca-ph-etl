-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.nca (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  release_id int NOT NULL,
  table_num int,
  nca_number text,
  nca_type text,
  agency text,
  department text,
  released_date text,
  purpose text,
  operating_unit text[],
  amount double precision[],
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_nca_release
    FOREIGN KEY (release_id) REFERENCES "release"(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
CREATE TABLE public.release (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  title text,
  filename text,
  url text,
  year int,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT release_pkey PRIMARY KEY (id)
);
