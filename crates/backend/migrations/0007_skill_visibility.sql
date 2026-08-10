-- 技能共享：与智能体一致的可见性模型（private / public_to / public）
ALTER TABLE public.skills
    ADD COLUMN visibility text NOT NULL DEFAULT 'private',
    ADD COLUMN public_to uuid[] NOT NULL DEFAULT '{}'::uuid[];

ALTER TABLE public.skills
    ADD CONSTRAINT skills_visibility_check
    CHECK (visibility = ANY (ARRAY['private'::text, 'public_to'::text, 'public'::text]));
