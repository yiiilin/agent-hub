-- 智能体端点暴露：控制该智能体可被哪些接入端点使用。
-- 默认全部开放（console / integration / automation），向后兼容。
ALTER TABLE public.agents
    ADD COLUMN endpoint_exposure text[] NOT NULL DEFAULT ARRAY['console', 'integration', 'automation'];
