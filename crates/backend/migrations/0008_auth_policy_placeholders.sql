-- 认证策略新增登录页占位符配置
ALTER TABLE public.auth_policy
    ADD COLUMN email_placeholder text NOT NULL DEFAULT '',
    ADD COLUMN password_placeholder text NOT NULL DEFAULT '';
