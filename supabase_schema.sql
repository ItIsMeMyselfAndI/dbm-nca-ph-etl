DROP TABLE IF EXISTS public.allocation CASCADE;
DROP TABLE IF EXISTS public.record CASCADE;
DROP TABLE IF EXISTS public.release CASCADE;

-- releases
CREATE TABLE public.release (
  id text PRIMARY KEY,
  title text,
  filename text,
  url text,
  year int,
  page_count int,
  file_meta_created_at text NOT NULL,
  file_meta_modified_at text NOT NULL,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

-- records
CREATE TABLE public.record (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nca_number text NOT NULL UNIQUE,
  nca_type text,
  department text,
  released_date timestamptz DEFAULT NULL,
  purpose text,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  release_id text NOT NULL REFERENCES public.release(id) ON DELETE CASCADE
);

-- operating units & amounts
CREATE TABLE public.allocation (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  operating_unit text NOT NULL,
  agency text NOT NULL,
  amount double precision NOT NULL,
  created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamptz DEFAULT CURRENT_TIMESTAMP,
  nca_number text NOT NULL REFERENCES public.record(nca_number) ON DELETE CASCADE
);

-- indeces
CREATE INDEX IF NOT EXISTS idx_release_id ON public.release(id);
CREATE INDEX IF NOT EXISTS idx_record_id ON public.record(id);
CREATE INDEX IF NOT EXISTS idx_record_nca_number ON public.record(nca_number);
CREATE INDEX IF NOT EXISTS idx_record_release_id ON public.record(release_id);
CREATE INDEX IF NOT EXISTS idx_allocation_id ON public.allocation(id);
CREATE INDEX IF NOT EXISTS idx_allocation_nca_number ON public.allocation(nca_number);
