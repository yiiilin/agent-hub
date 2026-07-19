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
  const username = uniqueSlug(context, prefix);
  return { email: `${username}@example.com`, username };
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

async function manualRedirect(client, path) {
  const headers = { accept: 'application/json' };
  const cookie = client.cookieHeader();
  if (cookie) headers.cookie = cookie;
  const response = await fetch(new URL(path, client.baseURL), {
    headers,
    redirect: 'manual'
  });
  client.absorbCookies(response.headers);
  assert.equal(response.status, 303, 'Mock OIDC step must return a manual redirect');
  const location = response.headers.get('location');
  assert.equal(typeof location, 'string', 'Mock OIDC step must include a redirect location');
  return location;
}

async function oidcLogin(context, email, subject) {
  const client = new ApiClient(context.baseURL);
  const callbackLocation = await manualRedirect(
    client,
    `/api/auth/oidc/mock/start?email=${encodeURIComponent(email)}&sub=${encodeURIComponent(subject)}`,
  );
  await manualRedirect(client, callbackLocation);
  assert.ok(client.cookies.size > 0, 'Mock OIDC callback must set a session cookie');
  const { data: user } = await client.get('/api/auth/me');
  return { client, user };
}

function bearerOptions(token, expectedStatus) {
  return {
    headers: { authorization: `Bearer ${token}` },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

export default async function identityAdministrationApiScenario(context) {
  const superClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(superClient);
  const { data: policySnapshot } = await superClient.get('/api/admin/auth-policy');

  const activePolicy = {
    password_registration_enabled: true,
    password_login_enabled: true,
    email_verification_required: false
  };

  try {
    await setPolicy(superClient, activePolicy);
    const { data: activeProviders } = await superClient.get('/api/auth/providers');
    assert.equal(activeProviders.password_registration_enabled, true);
    assert.equal(activeProviders.password_login_enabled, true);
    assert.equal(activeProviders.email_verification_required, false);
    assert.equal(activeProviders.oidc_mock, true);

    const memberIdentity = uniqueEmail(context, 'qa-api-member');
    const memberPassword = `${context.unique('Member password')}!Aa9`;
    const memberSession = new ApiClient(context.baseURL);
    const { data: registration } = await memberSession.post('/api/auth/register', {
      email: memberIdentity.email,
      password: memberPassword
    });
    assert.equal(registration.user.email, memberIdentity.email);
    assert.equal(registration.user.username, memberIdentity.username);
    assert.equal(registration.user.role, 'member');
    assert.equal(registration.verification_required, false);
    assert.equal((await memberSession.get('/api/auth/me')).data.id, registration.user.id);

    await memberSession.post('/api/auth/logout', undefined, { expectedStatus: 204 });
    await memberSession.get('/api/auth/me', { expectedStatus: 401 });
    const { data: memberLogin } = await memberSession.post('/api/auth/login', {
      email: memberIdentity.email,
      password: memberPassword
    });
    assert.equal(memberLogin.user.id, registration.user.id);
    assert.equal((await memberSession.get('/api/auth/me')).data.id, registration.user.id);

    await setPolicy(superClient, {
      ...activePolicy,
      password_registration_enabled: false
    });
    const blockedRegistration = uniqueEmail(context, 'qa-api-blocked-registration');
    await new ApiClient(context.baseURL).post('/api/auth/register', {
      email: blockedRegistration.email,
      password: `${context.unique('Blocked registration password')}!Aa9`
    }, { expectedStatus: 403 });

    await setPolicy(superClient, {
      ...activePolicy,
      password_login_enabled: false
    });
    await new ApiClient(context.baseURL).post('/api/auth/login', {
      email: memberIdentity.email,
      password: memberPassword
    }, { expectedStatus: 403 });

    await setPolicy(superClient, {
      ...activePolicy,
      email_verification_required: true
    });
    const verificationIdentity = uniqueEmail(context, 'qa-api-verification');
    const verificationClient = new ApiClient(context.baseURL);
    const { data: verificationRegistration } = await verificationClient.post('/api/auth/register', {
      email: verificationIdentity.email,
      password: `${context.unique('Verification password')}!Aa9`
    });
    assert.equal(verificationRegistration.verification_required, true);
    assert.equal(verificationRegistration.user.email, verificationIdentity.email);
    assert.equal(verificationRegistration.user.username, verificationIdentity.username);
    await verificationClient.get('/api/auth/me', { expectedStatus: 401 });
    await setPolicy(superClient, activePolicy);

    const oidcIdentity = uniqueEmail(context, 'qa-api-oidc');
    const oidcSubject = context.unique('qa-api-oidc-subject');
    const firstOidc = await oidcLogin(context, oidcIdentity.email, oidcSubject);
    assert.equal(firstOidc.user.email, oidcIdentity.email);
    const firstBinding = context.compose.psql(`
      SELECT id || '|' || platform_id || '|' || authentication_channel_id
      FROM external_identities
      WHERE user_id = ${sqlLiteral(firstOidc.user.id)}
        AND external_user_id = ${sqlLiteral(oidcSubject)}
    `);
    assert.match(firstBinding, new RegExp(`^${UUID_PATTERN.source.slice(1, -1)}\\|${UUID_PATTERN.source.slice(1, -1)}\\|${UUID_PATTERN.source.slice(1, -1)}$`));

    const secondOidc = await oidcLogin(context, oidcIdentity.email, oidcSubject);
    assert.equal(secondOidc.user.id, firstOidc.user.id);
    const secondBinding = context.compose.psql(`
      SELECT id || '|' || platform_id || '|' || authentication_channel_id
      FROM external_identities
      WHERE user_id = ${sqlLiteral(secondOidc.user.id)}
        AND external_user_id = ${sqlLiteral(oidcSubject)}
    `);
    assert.equal(secondBinding, firstBinding, 'Repeated Mock OIDC login must keep one stable binding');

    const platformKey = uniqueSlug(context, 'qa-api-platform');
    const platformName = context.unique('QA API Platform');
    const { data: platform } = await superClient.post('/api/admin/external-platforms', {
      key: platformKey,
      name: platformName
    });
    assert.match(platform.id, UUID_PATTERN);
    assert.equal(platform.key, platformKey);
    const { data: listedPlatforms } = await superClient.get('/api/admin/external-platforms');
    assert.equal(listedPlatforms.find((item) => item.id === platform.id)?.key, platformKey);

    const updatedPlatformName = context.unique('QA API Platform Updated');
    const { data: updatedPlatform } = await superClient.request(
      `/api/admin/external-platforms/${platform.id}`,
      { method: 'PATCH', body: { name: updatedPlatformName } }
    );
    assert.equal(updatedPlatform.id, platform.id);
    assert.equal(updatedPlatform.key, platformKey);
    assert.equal(updatedPlatform.name, updatedPlatformName);

    const channelKey = uniqueSlug(context, 'qa-api-channel');
    const channelName = context.unique('QA API Channel');
    const { data: channel } = await superClient.post(
      `/api/admin/external-platforms/${platform.id}/authentication-channels`,
      { key: channelKey, name: channelName, enabled: false, trusted_email: false }
    );
    assert.match(channel.id, UUID_PATTERN);
    assert.equal(channel.platform_id, platform.id);
    assert.equal(channel.key, channelKey);
    assert.equal(channel.enabled, false);
    assert.equal(channel.trusted_email, false);
    const { data: initialChannels } = await superClient.get(
      `/api/admin/external-platforms/${platform.id}/authentication-channels`
    );
    const initialChannel = initialChannels.find((item) => item.id === channel.id);
    assert.equal(initialChannel?.key, channelKey);
    assert.equal(initialChannel?.enabled, false);
    assert.equal(initialChannel?.trusted_email, false);

    const updatedChannelName = context.unique('QA API Channel Updated');
    const { data: updatedChannel } = await superClient.request(
      `/api/admin/authentication-channels/${channel.id}`,
      {
        method: 'PATCH',
        body: { name: updatedChannelName, enabled: true, trusted_email: true }
      }
    );
    assert.equal(updatedChannel.id, channel.id);
    assert.equal(updatedChannel.key, channelKey);
    assert.equal(updatedChannel.name, updatedChannelName);
    assert.equal(updatedChannel.enabled, true);
    assert.equal(updatedChannel.trusted_email, true);
    const { data: persistedChannels } = await superClient.get(
      `/api/admin/external-platforms/${platform.id}/authentication-channels`
    );
    const persistedChannel = persistedChannels.find((item) => item.id === channel.id);
    assert.equal(persistedChannel?.key, channelKey);
    assert.equal(persistedChannel?.enabled, true);
    assert.equal(persistedChannel?.trusted_email, true);

    const keyName = context.unique('QA API Key');
    const keyCreatedAfter = Date.now();
    const { data: createdKey } = await memberSession.post('/api/auth/api-keys', {
      name: keyName,
      validity: { kind: 'days', days: 180 }
    });
    const apiKey = createdKey.api_key;
    const token = createdKey.token;
    assert.match(apiKey.id, UUID_PATTERN);
    assert.equal(API_KEY_PATTERN.test(token), true, 'Created API key must have the expected format');
    assert.equal(token.startsWith(apiKey.prefix), true, 'Created API key prefix must match its token');
    const expirationDays = (new Date(apiKey.expires_at).getTime() - keyCreatedAfter) / 86_400_000;
    assert.ok(expirationDays > 179.9 && expirationDays <= 180.1, 'API key must use the explicit 180-day validity');

    const { data: keyList } = await memberSession.get('/api/auth/api-keys?page=1&page_size=100');
    const listedKey = keyList.items.find((item) => item.id === apiKey.id);
    assert.equal(listedKey?.prefix, apiKey.prefix);
    assert.equal(Object.hasOwn(listedKey ?? {}, 'token'), false, 'API key list must not expose a token field');
    assert.equal(JSON.stringify(keyList).includes(token), false, 'API key plaintext must not reappear in list responses');
    assert.equal(
      context.compose.psql(`SELECT count(*) FROM api_keys WHERE id = ${sqlLiteral(apiKey.id)} AND token_hash <> ''`),
      '1'
    );

    const keyClient = new ApiClient(context.baseURL);
    assert.equal((await keyClient.get('/api/auth/me', bearerOptions(token))).data.id, registration.user.id);
    const laterExpiration = new Date(new Date(apiKey.expires_at).getTime() + 90 * 86_400_000).toISOString();
    await keyClient.post(
      `/api/auth/api-keys/${apiKey.id}/renew`,
      { validity: { kind: 'date', expires_at: laterExpiration } },
      bearerOptions(token, 404)
    );
    await keyClient.delete(`/api/auth/api-keys/${apiKey.id}`, bearerOptions(token, 404));

    const { data: renewedKey } = await memberSession.post(
      `/api/auth/api-keys/${apiKey.id}/renew`,
      { validity: { kind: 'date', expires_at: laterExpiration } }
    );
    assert.equal(renewedKey.id, apiKey.id);
    assert.equal(renewedKey.prefix, apiKey.prefix);
    assert.equal(renewedKey.expires_at, laterExpiration);
    assert.equal(Object.hasOwn(renewedKey, 'token'), false, 'Renewal must not return API key plaintext');
    assert.equal((await keyClient.get('/api/auth/me', bearerOptions(token))).data.id, registration.user.id);

    const { data: memberDetail } = await superClient.get(`/api/admin/users/${registration.user.id}`);
    assert.equal(memberDetail.user.id, registration.user.id);
    assert.equal(memberDetail.user.role, 'member');
    assert.equal(memberDetail.has_password, true);

    const { data: promoted } = await superClient.request(`/api/admin/users/${registration.user.id}/role`, {
      method: 'PUT',
      body: { role: 'admin' }
    });
    assert.equal(promoted.user.role, 'admin');

    const adminClient = new ApiClient(context.baseURL);
    const { data: promotedLogin } = await adminClient.post('/api/auth/login', {
      email: memberIdentity.email,
      password: memberPassword
    });
    assert.equal(promotedLogin.user.role, 'admin');
    const { data: adminVisibleUsers } = await adminClient.get('/api/admin/users');
    assert.equal(adminVisibleUsers.some((detail) => detail.user.id === superAdmin.id), false);
    await adminClient.get(`/api/admin/users/${superAdmin.id}`, { expectedStatus: 404 });
    await adminClient.request(`/api/admin/users/${superAdmin.id}/password`, {
      method: 'PUT',
      body: { password: `${context.unique('Forbidden super password')}!Aa9` },
      expectedStatus: 404
    });
    await adminClient.request(`/api/admin/users/${superAdmin.id}/role`, {
      method: 'PUT',
      body: { role: 'member' },
      expectedStatus: 403
    });

    await superClient.request(`/api/admin/users/${superAdmin.id}/role`, {
      method: 'PUT',
      body: { role: 'admin' },
      expectedStatus: 409
    });
    assert.equal((await superClient.get(`/api/admin/users/${superAdmin.id}`)).data.user.role, 'super_admin');

    const { data: demoted } = await superClient.request(`/api/admin/users/${registration.user.id}/role`, {
      method: 'PUT',
      body: { role: 'member' }
    });
    assert.equal(demoted.user.role, 'member');

    const replacementPassword = `${context.unique('Replacement password')}!Aa9`;
    const { data: passwordUpdated } = await superClient.request(
      `/api/admin/users/${registration.user.id}/password`,
      { method: 'PUT', body: { password: replacementPassword } }
    );
    assert.equal(passwordUpdated.user.id, registration.user.id);
    assert.equal(passwordUpdated.has_password, true);
    await memberSession.get('/api/auth/me', { expectedStatus: 401 });
    await adminClient.get('/api/auth/me', { expectedStatus: 401 });
    assert.equal((await keyClient.get('/api/auth/me', bearerOptions(token))).data.id, registration.user.id);
    await new ApiClient(context.baseURL).post('/api/auth/login', {
      email: memberIdentity.email,
      password: memberPassword
    }, { expectedStatus: 401 });

    const replacementSession = new ApiClient(context.baseURL);
    const { data: replacementLogin } = await replacementSession.post('/api/auth/login', {
      email: memberIdentity.email,
      password: replacementPassword
    });
    assert.equal(replacementLogin.user.id, registration.user.id);
    await replacementSession.delete(`/api/auth/api-keys/${apiKey.id}`, { expectedStatus: 204 });
    await keyClient.get('/api/auth/me', bearerOptions(token, 401));
    const { data: keysAfterDelete } = await replacementSession.get('/api/auth/api-keys?page=1&page_size=100');
    assert.equal(keysAfterDelete.items.some((item) => item.id === apiKey.id), false);
    assert.equal(
      context.compose.psql(`SELECT count(*) FROM api_keys WHERE id = ${sqlLiteral(apiKey.id)}`),
      '0'
    );

    await superClient.post(
      `/api/admin/users/${registration.user.id}/erase`,
      { username: `${memberIdentity.username}-wrong` },
      { expectedStatus: 409 }
    );
    const { data: erasure } = await superClient.post(
      `/api/admin/users/${registration.user.id}/erase`,
      { username: memberIdentity.username },
      { expectedStatus: 202 }
    );
    assert.equal(erasure.user_id, registration.user.id);
    const completedErasure = await poll(async () => {
      const { data: history } = await superClient.get('/api/admin/user-erasures');
      return history.find((item) => item.user_id === registration.user.id) ?? null;
    }, (item) => item?.status === 'completed', {
      timeoutMs: 45_000,
      description: 'unique member erasure to complete'
    });
    assert.equal(completedErasure.user_id, registration.user.id);
    assert.equal(completedErasure.status, 'completed');
    assert.ok(completedErasure.completed_at);
    await superClient.get(`/api/admin/users/${registration.user.id}`, { expectedStatus: 404 });
  } finally {
    const restored = await setPolicy(superClient, policySnapshot);
    assert.deepEqual(restored, policySnapshot);
    const { data: persistedPolicy } = await superClient.get('/api/admin/auth-policy');
    assert.deepEqual(persistedPolicy, policySnapshot);
  }
}
