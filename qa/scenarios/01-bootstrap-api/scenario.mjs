import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';

export default async function bootstrapApiScenario(context) {
  const health = await fetch(`${context.baseURL}/healthz`);
  assert.equal(health.status, 200);

  const client = new ApiClient(context.baseURL);
  const { data: user } = await loginAsAdmin(client);
  assert.equal(user.email, 'admin@example.com');
  assert.equal(user.role, 'super_admin');

  const { data: runtimes } = await client.get('/api/runtimes');
  assert.ok(Array.isArray(runtimes));
  assert.ok(runtimes.some((runtime) => runtime.status === 'online'));

  const { data: systemDefault } = await client.get('/api/model-connections/system-default');
  assert.match(systemDefault.selection?.connection_id, /^[0-9a-f-]{36}$/);
  assert.equal(typeof systemDefault.selection?.model_id, 'string');

  assert.equal(
    context.compose.psql("SELECT version || '|' || description || '|' || success FROM _sqlx_migrations ORDER BY version"),
    '1|initial|true'
  );
  assert.equal(
    context.compose.psql("SELECT password_registration_enabled || '|' || password_login_enabled || '|' || ldap_login_enabled FROM auth_policy WHERE singleton = true"),
    'false|true|false'
  );
  assert.equal(
    context.compose.psql("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users'"),
    'id,email,password,display_name,role,created_at,deletion_requested_at'
  );
  assert.equal(
    context.compose.psql("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'ldap_configuration'"),
    'singleton,url,security_mode,base_dn,bind_identity_template,user_filter,email_attribute,display_name_attribute,allow_insecure,skip_tls_verify,updated_by,updated_at'
  );
  assert.equal(
    context.compose.psql("SELECT requested_email FROM user_erasure_jobs LIMIT 0"),
    ''
  );
}
