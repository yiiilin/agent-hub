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
    context.compose.psql("SELECT password_registration_enabled || '|' || password_login_enabled || '|' || email_verification_required FROM auth_policy WHERE singleton = true"),
    'true|true|false'
  );
}
