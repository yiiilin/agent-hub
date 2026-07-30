import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const API_KEY_PATTERN = /^ahk_[A-Za-z0-9]+$/;

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function uniqueEmail(context, prefix) {
  return `${uniqueSlug(context, prefix)}@example.com`;
}

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

async function setPolicy(client, policy) {
  const { data } = await client.request('/api/admin/auth-policy', {
    method: 'PATCH',
    body: policy
  });
  assert.deepEqual(data, policy);
  return data;
}

function bearerOptions(token, expectedStatus) {
  return {
    headers: { authorization: `Bearer ${token}` },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

async function oauthToken(baseURL, form, expectedStatus = 200) {
  const response = await fetch(new URL('/api/oauth/token', baseURL), {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(form)
  });
  assert.equal(response.status, expectedStatus, `OAuth token endpoint returned ${response.status}`);
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

async function restoreLdapConfiguration(context, client, snapshot) {
  if (snapshot) {
    const { data } = await client.request('/api/admin/ldap-config', {
      method: 'PUT',
      body: snapshot
    });
    assert.deepEqual(data, snapshot, 'LDAP configuration restore response must match snapshot');
    return;
  }
  context.compose.psql('DELETE FROM ldap_configuration WHERE singleton = true');
  assert.equal(
    context.compose.psql('SELECT count(*) FROM ldap_configuration WHERE singleton = true'),
    '0',
    'Scenario-owned LDAP policy fixture must be removed'
  );
}

async function runCleanupSteps(steps) {
  const errors = [];
  for (const [label, cleanup] of steps) {
    try {
      await cleanup();
    } catch (error) {
      errors.push(new Error(`${label}: ${error instanceof Error ? error.message : String(error)}`));
    }
  }
  if (errors.length > 0) throw new AggregateError(errors, 'Identity administration cleanup failed');
}

export default async function identityAdministrationApiScenario(context) {
  const superClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(superClient);
  const { data: policySnapshot } = await superClient.get('/api/admin/auth-policy');
  const { data: ldapSnapshot } = await superClient.get('/api/admin/ldap-config');
  const activePolicy = {
    password_registration_enabled: true,
    password_login_enabled: true,
    ldap_login_enabled: false
  };
  let integrationAgentId = null;

  try {
    await setPolicy(superClient, activePolicy);
    const { data: providers } = await superClient.get('/api/auth/providers');
    assert.deepEqual(providers, activePolicy);

    const registrationEmail = uniqueEmail(context, 'qa-registration-member');
    const registrationPassword = `${context.unique('Registration password')}!Aa9`;
    const registrationDisplayName = context.unique('Registered Display Name');
    const registrationClient = new ApiClient(context.baseURL);
    const { data: registration } = await registrationClient.post('/api/auth/register', {
      email: registrationEmail,
      password: registrationPassword,
      display_name: registrationDisplayName
    });
    assert.deepEqual(registration.user, {
      id: registration.user.id,
      email: registrationEmail,
      display_name: registrationDisplayName,
      role: 'member'
    });
    assert.match(registration.user.id, UUID_PATTERN);
    assert.deepEqual((await superClient.get('/api/admin/auth-policy')).data, activePolicy);

    const selfDisplayName = context.unique('Self-managed Display Name');
    const { data: selfUpdated } = await registrationClient.request('/api/users/me', {
      method: 'PATCH',
      body: { display_name: selfDisplayName }
    });
    assert.equal(selfUpdated.display_name, selfDisplayName);
    assert.equal(selfUpdated.email, registrationEmail);

    await registrationClient.post('/api/auth/logout', undefined, { expectedStatus: 204 });
    await registrationClient.get('/api/auth/me', { expectedStatus: 401 });
    const { data: registrationLogin } = await registrationClient.post('/api/auth/login', {
      email: registrationEmail,
      password: registrationPassword
    });
    assert.equal(registrationLogin.user.id, registration.user.id);

    const registrationDisabled = { ...activePolicy, password_registration_enabled: false };
    await setPolicy(superClient, registrationDisabled);
    await new ApiClient(context.baseURL).post('/api/auth/register', {
      email: uniqueEmail(context, 'qa-registration-disabled'),
      password: `${context.unique('Blocked registration password')}!Aa9`
    }, { expectedStatus: 403 });
    await superClient.request('/api/admin/auth-policy', {
      method: 'PATCH',
      body: {
        password_registration_enabled: true,
        password_login_enabled: false,
        ldap_login_enabled: true
      },
      expectedStatus: 409
    });
    await superClient.request('/api/admin/auth-policy', {
      method: 'PATCH',
      body: {
        password_registration_enabled: false,
        password_login_enabled: false,
        ldap_login_enabled: false
      },
      expectedStatus: 409
    });
    assert.deepEqual((await superClient.get('/api/admin/auth-policy')).data, registrationDisabled);

    if (!ldapSnapshot) {
      await superClient.request('/api/admin/auth-policy', {
        method: 'PATCH',
        body: {
          password_registration_enabled: false,
          password_login_enabled: true,
          ldap_login_enabled: true
        },
        expectedStatus: 409
      });
    }
    const policyOnlyLdapConfiguration = {
      url: 'ldap://127.0.0.1:1',
      security: 'plain',
      base_dn: 'ou=people,dc=example,dc=test',
      bind_identity_template: '{email}',
      user_filter: '(mail={email})',
      email_attribute: 'mail',
      display_name_attribute: 'displayName',
      allow_insecure: true,
      skip_tls_verify: false
    };
    await superClient.request('/api/admin/ldap-config', {
      method: 'PUT',
      body: policyOnlyLdapConfiguration
    });
    await superClient.post('/api/admin/ldap-config/test', {
      configuration: {
        ...policyOnlyLdapConfiguration,
        bind_identity_template: 'uid=missing-placeholder,ou=people,dc=example,dc=test'
      },
      email: superAdmin.email,
      password: 'one-time-validation-only'
    }, { expectedStatus: 400 });
    await setPolicy(superClient, {
      password_registration_enabled: false,
      password_login_enabled: false,
      ldap_login_enabled: true
    });
    await new ApiClient(context.baseURL).post('/api/auth/login', {
      email: registrationEmail,
      password: registrationPassword
    }, { expectedStatus: 403 });
    const emergencyClient = new ApiClient(context.baseURL);
    const { data: emergencyLogin } = await emergencyClient.post('/api/auth/login', {
      email: superAdmin.email,
      password: 'admin123'
    });
    assert.equal(emergencyLogin.user.id, superAdmin.id);
    assert.equal(emergencyLogin.user.role, 'super_admin');
    await setPolicy(superClient, activePolicy);

    const administratorEmail = uniqueEmail(context, 'qa-created-administrator');
    const administratorPassword = `${context.unique('Administrator password')}!Bb8`;
    const { data: administratorDetail } = await superClient.post('/api/admin/users', {
      email: administratorEmail,
      display_name: context.unique('Created Administrator'),
      password: administratorPassword,
      role: 'admin'
    });
    assert.equal(administratorDetail.user.role, 'admin');
    assert.equal(administratorDetail.has_password, true);
    const administratorClient = new ApiClient(context.baseURL);
    assert.equal((await administratorClient.post('/api/auth/login', {
      email: administratorEmail,
      password: administratorPassword
    })).data.user.id, administratorDetail.user.id);

    const managedEmail = uniqueEmail(context, 'qa-admin-created-member');
    const managedPassword = `${context.unique('Managed member password')}!Cc7`;
    const { data: managedDetail } = await administratorClient.post('/api/admin/users', {
      email: managedEmail,
      display_name: context.unique('Admin-created Member'),
      password: managedPassword,
      role: 'member'
    });
    assert.equal(managedDetail.user.role, 'member');
    await administratorClient.post('/api/admin/users', {
      email: uniqueEmail(context, 'qa-admin-forbidden-admin'),
      role: 'admin'
    }, { expectedStatus: 403 });
    assert.equal(
      (await administratorClient.get('/api/admin/users')).data.some(
        (detail) => detail.user.id === superAdmin.id
      ),
      false
    );
    await administratorClient.get(`/api/admin/users/${superAdmin.id}`, { expectedStatus: 404 });
    await administratorClient.request(`/api/admin/users/${managedDetail.user.id}/role`, {
      method: 'PUT',
      body: { role: 'admin' },
      expectedStatus: 403
    });

    const managedSession = new ApiClient(context.baseURL);
    await managedSession.post('/api/auth/login', { email: managedEmail, password: managedPassword });
    const managedSelfName = context.unique('Managed Self Display Name');
    assert.equal((await managedSession.request('/api/users/me', {
      method: 'PATCH',
      body: { display_name: managedSelfName }
    })).data.display_name, managedSelfName);

    const { data: createdKey } = await managedSession.post('/api/auth/api-keys', {
      name: context.unique('Managed retained API key'),
      validity: { kind: 'days', days: 180 }
    });
    assert.match(createdKey.api_key.id, UUID_PATTERN);
    assert.equal(API_KEY_PATTERN.test(createdKey.token), true);
    const keyClient = new ApiClient(context.baseURL);
    assert.equal(
      (await keyClient.get('/api/auth/me', bearerOptions(createdKey.token))).data.id,
      managedDetail.user.id
    );

    await superClient.request(`/api/admin/users/${managedDetail.user.id}`, {
      method: 'PATCH',
      body: { email: registrationEmail, display_name: managedSelfName },
      expectedStatus: 409
    });
    assert.equal((await managedSession.get('/api/auth/me')).data.id, managedDetail.user.id);

    const updatedManagedEmail = uniqueEmail(context, 'qa-updated-managed-member');
    const updatedManagedDisplayName = context.unique('Updated Managed Display Name');
    const { data: updatedManaged } = await superClient.request(
      `/api/admin/users/${managedDetail.user.id}`,
      {
        method: 'PATCH',
        body: { email: updatedManagedEmail, display_name: updatedManagedDisplayName }
      }
    );
    assert.equal(updatedManaged.user.email, updatedManagedEmail);
    assert.equal(updatedManaged.user.display_name, updatedManagedDisplayName);
    await managedSession.get('/api/auth/me', { expectedStatus: 401 });
    assert.equal(
      (await keyClient.get('/api/auth/me', bearerOptions(createdKey.token))).data.id,
      managedDetail.user.id,
      'Email changes must retain API keys'
    );
    await new ApiClient(context.baseURL).post('/api/auth/login', {
      email: managedEmail,
      password: managedPassword
    }, { expectedStatus: 401 });
    const managedReplacementSession = new ApiClient(context.baseURL);
    await managedReplacementSession.post('/api/auth/login', {
      email: updatedManagedEmail,
      password: managedPassword
    });

    const { data: promotedManaged } = await superClient.request(
      `/api/admin/users/${managedDetail.user.id}/role`,
      { method: 'PUT', body: { role: 'admin' } }
    );
    assert.equal(promotedManaged.user.role, 'admin');
    await superClient.request(`/api/admin/users/${superAdmin.id}/role`, {
      method: 'PUT',
      body: { role: 'admin' },
      expectedStatus: 409
    });
    assert.equal(
      (await superClient.get(`/api/admin/users/${superAdmin.id}`)).data.user.role,
      'super_admin'
    );

    const replacementPassword = `${context.unique('Replacement managed password')}!Dd6`;
    const { data: passwordUpdated } = await superClient.request(
      `/api/admin/users/${managedDetail.user.id}/password`,
      { method: 'PUT', body: { password: replacementPassword } }
    );
    assert.equal(passwordUpdated.has_password, true);
    await managedReplacementSession.get('/api/auth/me', { expectedStatus: 401 });
    assert.equal(
      (await keyClient.get('/api/auth/me', bearerOptions(createdKey.token))).data.id,
      managedDetail.user.id,
      'Password changes must retain API keys'
    );
    await new ApiClient(context.baseURL).post('/api/auth/login', {
      email: updatedManagedEmail,
      password: managedPassword
    }, { expectedStatus: 401 });
    const replacementSession = new ApiClient(context.baseURL);
    assert.equal((await replacementSession.post('/api/auth/login', {
      email: updatedManagedEmail,
      password: replacementPassword
    })).data.user.id, managedDetail.user.id);
    await replacementSession.delete(`/api/auth/api-keys/${createdKey.api_key.id}`, {
      expectedStatus: 204
    });
    await keyClient.get('/api/auth/me', bearerOptions(createdKey.token, 401));

    const platformKey = uniqueSlug(context, 'qa-trusted-email-platform');
    const { data: platform } = await superClient.post('/api/admin/external-platforms', {
      key: platformKey,
      name: context.unique('QA Trusted Email Platform')
    });
    const { data: channel } = await superClient.post(
      `/api/admin/external-platforms/${platform.id}/authentication-channels`,
      {
        key: uniqueSlug(context, 'qa-trusted-email-channel'),
        name: context.unique('QA Trusted Email Channel'),
        enabled: true,
        trusted_email: true
      }
    );
    const { data: integrationAgent } = await superClient.post('/api/agents', {
      name: context.unique('QA Trusted Email Agent'),
      instructions: 'Validate trusted email identity binding.',
      visibility: 'private',
      public_to: []
    });
    integrationAgentId = integrationAgent.id;
    const { data: appSecret } = await superClient.post('/api/integration-apps', {
      name: context.unique('QA Trusted Email App'),
      external_platform_id: platform.id,
      authentication_channel_id: channel.id,
      redirect_uris: [new URL('/qa/trusted-email-callback', context.baseURL).href],
      agent_ids: [integrationAgent.id]
    });
    const appToken = await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: appSecret.integration_app.client_id,
      client_secret: appSecret.client_secret,
      scope: `agent:${integrationAgent.id}`
    });
    const externalUserId = uniqueSlug(context, 'qa-external-user');
    const externalUsername = context.unique('External profile username');
    const integrationBody = {
      agent_id: integrationAgent.id,
      external_user_id: externalUserId,
      tenant_id: 'qa-trusted-email',
      username: externalUsername,
      display_name: context.unique('External Profile Display Name'),
      tools: [],
      metadata: { source: 'qa-trusted-email' }
    };
    await new ApiClient(context.baseURL).post(
      '/api/integrations/sessions',
      integrationBody,
      bearerOptions(appToken.access_token, 400)
    );
    await new ApiClient(context.baseURL).post(
      '/api/integrations/sessions',
      { ...integrationBody, email: 'not-an-email' },
      bearerOptions(appToken.access_token, 400)
    );
    const { data: integrationSession } = await new ApiClient(context.baseURL).post(
      '/api/integrations/sessions',
      { ...integrationBody, email: registrationEmail },
      bearerOptions(appToken.access_token)
    );
    assert.match(integrationSession.external_identity_id, UUID_PATTERN);
    assert.equal(
      context.compose.psql(`
        SELECT user_id || '|' || last_username
        FROM external_identities
        WHERE id = ${sqlLiteral(integrationSession.external_identity_id)}
      `),
      `${registration.user.id}|${externalUsername}`,
      'Trusted Integration email must bind the existing Hub user while retaining external username profile data'
    );

    await superClient.post(
      `/api/admin/users/${managedDetail.user.id}/erase`,
      { email: `${updatedManagedEmail}.wrong` },
      { expectedStatus: 409 }
    );
    const { data: erasure } = await superClient.post(
      `/api/admin/users/${managedDetail.user.id}/erase`,
      { email: updatedManagedEmail },
      { expectedStatus: 202 }
    );
    assert.equal(erasure.user_id, managedDetail.user.id);
    if (erasure.email !== null) assert.equal(erasure.email, updatedManagedEmail);
    const completedErasure = await poll(async () => {
      const { data: history } = await superClient.get('/api/admin/user-erasures');
      return history.find((item) => item.user_id === managedDetail.user.id) ?? null;
    }, (item) => item?.status === 'completed', {
      timeoutMs: 45_000,
      description: 'email-confirmed managed user erasure to complete'
    });
    assert.equal(completedErasure.email, null, 'Completed erasure history must not retain the email');
    assert.ok(completedErasure.completed_at);
    await superClient.get(`/api/admin/users/${managedDetail.user.id}`, { expectedStatus: 404 });
  } finally {
    const authRestores = ldapSnapshot
      ? [
          ['restore LDAP configuration', () => restoreLdapConfiguration(context, superClient, ldapSnapshot)],
          ['restore authentication policy', () => setPolicy(superClient, policySnapshot)]
        ]
      : [
          ['restore authentication policy', () => setPolicy(superClient, policySnapshot)],
          ['remove scenario LDAP configuration', () => restoreLdapConfiguration(context, superClient, null)]
        ];
    await runCleanupSteps([
      ...(integrationAgentId
        ? [[
            'delete trusted-email integration Agent',
            () => superClient.delete(`/api/agents/${integrationAgentId}`, { expectedStatus: [204, 404] })
          ]]
        : []),
      ...authRestores,
      [
        'verify restored authentication state',
        async () => {
          assert.deepEqual((await superClient.get('/api/admin/auth-policy')).data, policySnapshot);
          assert.deepEqual((await superClient.get('/api/admin/ldap-config')).data, ldapSnapshot);
        }
      ]
    ]);
  }
}
