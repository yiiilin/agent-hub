-- 系统参数：附件大小限制等运行时配置
CREATE TABLE public.system_settings (
    singleton boolean DEFAULT true NOT NULL,
    max_attachment_upload_bytes bigint NOT NULL,
    max_attachment_bytes_per_session bigint NOT NULL,
    updated_by uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT system_settings_singleton_check CHECK (singleton)
);

INSERT INTO public.system_settings
    (singleton, max_attachment_upload_bytes, max_attachment_bytes_per_session)
VALUES (true, 104857600, 524288000);
