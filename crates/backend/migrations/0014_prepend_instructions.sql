-- 第三方应用会话预指令：创建会话时一次性写入、不可变。
ALTER TABLE public.integration_sessions
    ADD COLUMN prepend_instructions TEXT
    CHECK (prepend_instructions IS NULL OR octet_length(prepend_instructions) <= 65536);
