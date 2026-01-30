-- releases
CREATE TABLE public."release" (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  title text,
  filename text,
  url text,
  year int,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

-- records
CREATE TABLE public.record (
  id int PRIMARY KEY,
  nca_number text NOT NULL,
  table_num int,
  nca_type text,
  department text,
  released_date text,
  purpose text,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  release_id int NOT NULL REFERENCES public."release"(id) ON DELETE CASCADE
);

-- operating units & amounts
CREATE TABLE public.allocation (
  id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  operating_unit text NOT NULL,
  agency text NOT NULL,
  amount double precision NOT NULL,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  record_id int NOT NULL REFERENCES public."record"(id) ON DELETE CASCADE
);

-- indeces
CREATE INDEX IF NOT EXISTS idx_release_id ON public.release(id);
CREATE INDEX IF NOT EXISTS idx_record_id ON public.record(id);
CREATE INDEX IF NOT EXISTS idx_allocation_id ON public.allocation(id);
CREATE INDEX IF NOT EXISTS idx_record_release_id 
ON public.record(release_id);
CREATE INDEX IF NOT EXISTS idx_allocation_record_id 
ON public.allocation(record_id);
