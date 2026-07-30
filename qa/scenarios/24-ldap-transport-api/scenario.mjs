import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';

const BASE_DN = 'ou=people,dc=example,dc=test';
const BIND_IDENTITY_TEMPLATE = 'uid={email},ou=people,dc=example,dc=test';
const STANDARD_USER = {
  email: 'qa.ldap@example.test',
  password: 'qa-ldap-password',
  displayName: 'QA LDAP User'
};
const ESCAPING_USER = {
  email: 'qa+filter*escape@example.test',
  password: 'qa-ldap-escaping-password',
  displayName: 'QA LDAP Escaping User'
};
const FILTER_WILDCARD_DECOY = {
  email: 'qa+filter-decoy-escape@example.test',
  password: 'qa-ldap-filter-decoy-password',
  displayName: 'QA LDAP Filter Wildcard Decoy'
};
const MAPPED_USER = {
  email: 'mapped.login@example.test',
  password: 'mapped-ldap-password',
  authoritativeEmail: 'canonical.user@example.test',
  displayName: 'Canonical LDAP User'
};
const NO_DISPLAY_USER = {
  email: 'no-display@example.test',
  password: 'no-display-password',
  displayName: 'no-display'
};
const DUPLICATE_USER = {
  email: 'duplicate@example.test',
  password: 'duplicate-password'
};

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function ldapConfiguration({
  security = 'plain',
  url = security === 'ldaps' ? 'ldaps://openldap:636' : 'ldap://openldap:389',
  bindIdentityTemplate = BIND_IDENTITY_TEMPLATE,
  userFilter = '(uid={email})',
  skipTlsVerify = false
} = {}) {
  return {
    url,
    security,
    base_dn: BASE_DN,
    bind_identity_template: bindIdentityTemplate,
    user_filter: userFilter,
    email_attribute: 'mail',
    display_name_attribute: 'displayName',
    allow_insecure: security === 'plain',
    skip_tls_verify: skipTlsVerify
  };
}

async function setPolicy(client, policy) {
  const { data } = await client.request('/api/admin/auth-policy', {
    method: 'PATCH',
    body: policy
  });
  assert.deepEqual(data, policy);
}

async function saveConfiguration(client, configuration) {
  const { data } = await client.request('/api/admin/ldap-config', {
    method: 'PUT',
    body: configuration
  });
  assert.deepEqual(data, configuration);
}

async function testConfiguration(client, configuration, identity, expectedStatus = 200) {
  return client.post('/api/admin/ldap-config/test', {
    configuration,
    email: identity.email,
    password: identity.password
  }, { expectedStatus });
}

async function ldapLogin(baseURL, identity, expectedStatus = 200, headers = {}) {
  const client = new ApiClient(baseURL);
  const response = await client.post('/api/auth/ldap/login', {
    email: identity.email,
    password: identity.password
  }, { expectedStatus, headers });
  return { client, ...response };
}

function assertRetryAfter(response) {
  const value = Number(response.headers.get('retry-after'));
  assert.ok(Number.isInteger(value) && value >= 1 && value <= 300, 'Retry-After must describe the active five-minute window');
}

function assertGenericLoginFailure(response) {
  assert.deepEqual(response.data, { error: 'invalid email or password' });
}

function assertSanitizedAdministratorFailure(response, identity) {
  assert.equal(typeof response.data?.error, 'string');
  assert.match(response.data.error, /^LDAP (bind|connect|mapping|search|timeout) failed:/);
  const serialized = JSON.stringify(response.data);
  for (const forbidden of [identity.password, BIND_IDENTITY_TEMPLATE, BASE_DN, 'uid=']) {
    assert.equal(serialized.includes(forbidden), false, `LDAP diagnostic must not expose ${forbidden}`);
  }
}

function blackholeUrl(context) {
  const subnet = context.compose.environment.HUB_NETWORK_SUBNET;
  const match = /^(\d+\.\d+\.\d+)\.0\/24$/.exec(subnet);
  assert.ok(match, `Unexpected QA Hub network subnet: ${subnet}`);
  return `ldap://${match[1]}.254:389`;
}

async function restoreLdapConfiguration(context, client, snapshot) {
  if (snapshot) {
    await saveConfiguration(client, snapshot);
  } else {
    context.compose.psql('DELETE FROM ldap_configuration WHERE singleton = true');
    assert.equal(context.compose.psql('SELECT count(*) FROM ldap_configuration'), '0');
  }
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
  if (errors.length > 0) throw new AggregateError(errors, 'Real LDAP API cleanup failed');
}

export default async function ldapTransportApiScenario(context) {
  const admin = new ApiClient(context.baseURL);
  await loginAsAdmin(admin);
  const { data: policySnapshot } = await admin.get('/api/admin/auth-policy');
  const { data: ldapSnapshot } = await admin.get('/api/admin/ldap-config');
  const trackedEmails = new Set([
    STANDARD_USER.email,
    ESCAPING_USER.email,
    FILTER_WILDCARD_DECOY.email,
    MAPPED_USER.email,
    MAPPED_USER.authoritativeEmail,
    NO_DISPLAY_USER.email,
    DUPLICATE_USER.email
  ]);
  const trackedIps = new Set();
  const enabledPolicy = {
    password_registration_enabled: false,
    password_login_enabled: true,
    ldap_login_enabled: true
  };

  try {
    const { data: openapi } = await new ApiClient(context.baseURL).get('/openapi.json');
    const ldapSchema = openapi.components.schemas.LdapConfiguration;
    assert.ok(ldapSchema.required.includes('bind_identity_template'));
    assert.equal(ldapSchema.properties.bind_identity_template.default, '{email}');

    const plain = ldapConfiguration();
    await admin.request('/api/admin/ldap-config', {
      method: 'PUT',
      body: { ...plain, bind_identity_template: `uid=missing-placeholder,${BASE_DN}` },
      expectedStatus: 400
    });
    await admin.request('/api/admin/ldap-config', {
      method: 'PUT',
      body: { ...plain, bind_identity_template: `{email},uid={email},${BASE_DN}` },
      expectedStatus: 400
    });

    const { data: plainDraft } = await testConfiguration(admin, plain, STANDARD_USER);
    assert.equal(plainDraft.email, STANDARD_USER.email);
    assert.equal(plainDraft.display_name, STANDARD_USER.displayName);
    assert.ok(Number.isInteger(plainDraft.duration_ms) && plainDraft.duration_ms >= 0);
    assert.deepEqual((await admin.get('/api/admin/ldap-config')).data, ldapSnapshot);

    await saveConfiguration(admin, plain);
    await setPolicy(admin, enabledPolicy);
    const standardSession = await ldapLogin(context.baseURL, STANDARD_USER);
    assert.equal(standardSession.data.user.email, STANDARD_USER.email);
    assert.equal(standardSession.data.user.display_name, STANDARD_USER.displayName);
    assert.equal(standardSession.data.user.role, 'member');
    assert.equal((await standardSession.client.get('/api/auth/me')).data.id, standardSession.data.user.id);

    const { data: escapingDraft } = await testConfiguration(admin, plain, ESCAPING_USER);
    assert.equal(escapingDraft.email, ESCAPING_USER.email);
    assert.equal(escapingDraft.display_name, ESCAPING_USER.displayName);
    const escapingSession = await ldapLogin(context.baseURL, ESCAPING_USER);
    assert.equal(escapingSession.data.user.email, ESCAPING_USER.email);
    assert.equal(escapingSession.data.user.display_name, ESCAPING_USER.displayName);
    const decoySession = await ldapLogin(context.baseURL, FILTER_WILDCARD_DECOY);
    assert.equal(decoySession.data.user.email, FILTER_WILDCARD_DECOY.email);
    assert.equal(decoySession.data.user.display_name, FILTER_WILDCARD_DECOY.displayName);

    const mappedSession = await ldapLogin(context.baseURL, MAPPED_USER);
    assert.equal(mappedSession.data.user.email, MAPPED_USER.authoritativeEmail);
    assert.equal(mappedSession.data.user.display_name, MAPPED_USER.displayName);
    const noDisplaySession = await ldapLogin(context.baseURL, NO_DISPLAY_USER);
    assert.equal(noDisplaySession.data.user.email, NO_DISPLAY_USER.email);
    assert.equal(noDisplaySession.data.user.display_name, NO_DISPLAY_USER.displayName);

    const ipAttemptsBeforeDraftTests = context.compose.psql(
      'SELECT COALESCE(sum(attempts), 0) FROM login_ip_attempts'
    );
    const zeroResult = await testConfiguration(
      admin,
      ldapConfiguration({ userFilter: '(mail=absent-{email})' }),
      STANDARD_USER,
      400
    );
    assertSanitizedAdministratorFailure(zeroResult, STANDARD_USER);
    await testConfiguration(admin, plain, STANDARD_USER);

    const multipleResults = await testConfiguration(
      admin,
      ldapConfiguration({ userFilter: '(description={email})' }),
      DUPLICATE_USER,
      400
    );
    assertSanitizedAdministratorFailure(multipleResults, DUPLICATE_USER);
    await testConfiguration(admin, plain, DUPLICATE_USER);
    assert.equal(
      context.compose.psql('SELECT COALESCE(sum(attempts), 0) FROM login_ip_attempts'),
      ipAttemptsBeforeDraftTests,
      'LDAP draft tests must not consume the ordinary source-IP login budget'
    );

    const zeroLoginConfiguration = ldapConfiguration({ userFilter: '(mail=absent-{email})' });
    await saveConfiguration(admin, zeroLoginConfiguration);
    const zeroLogin = await ldapLogin(context.baseURL, STANDARD_USER, 401);
    assertGenericLoginFailure(zeroLogin);
    await saveConfiguration(admin, plain);
    await ldapLogin(context.baseURL, STANDARD_USER);
    assert.equal((await standardSession.client.get('/api/auth/me')).data.id, standardSession.data.user.id);

    for (const security of ['starttls', 'ldaps']) {
      const verified = ldapConfiguration({ security });
      const rejected = await testConfiguration(admin, verified, STANDARD_USER, 503);
      assertSanitizedAdministratorFailure(rejected, STANDARD_USER);
      const skipped = ldapConfiguration({ security, skipTlsVerify: true });
      const { data: accepted } = await testConfiguration(admin, skipped, STANDARD_USER);
      assert.equal(accepted.email, STANDARD_USER.email);
      await saveConfiguration(admin, skipped);
      const tlsSession = await ldapLogin(context.baseURL, STANDARD_USER);
      assert.equal(tlsSession.data.user.id, standardSession.data.user.id);
      assert.equal((await standardSession.client.get('/api/auth/me')).data.id, standardSession.data.user.id);
    }

    const persistedTlsConfiguration = (await admin.get('/api/admin/ldap-config')).data;
    const timeoutConfiguration = ldapConfiguration({ url: blackholeUrl(context) });
    const timeoutStartedAt = Date.now();
    const timedOut = await testConfiguration(admin, timeoutConfiguration, STANDARD_USER, 503);
    const timeoutDurationMs = Date.now() - timeoutStartedAt;
    assertSanitizedAdministratorFailure(timedOut, STANDARD_USER);
    assert.ok(timeoutDurationMs >= 2_500, `LDAP timeout path returned too early: ${timeoutDurationMs} ms`);
    assert.ok(timeoutDurationMs < 9_000, `LDAP timeout path suggests retry or deadline drift: ${timeoutDurationMs} ms`);
    assert.deepEqual((await admin.get('/api/admin/ldap-config')).data, persistedTlsConfiguration);
    await testConfiguration(admin, plain, STANDARD_USER);

    await setPolicy(admin, { ...enabledPolicy, ldap_login_enabled: false });
    const disabledEmail = `${uniqueSlug(context, 'qa-ldap-disabled')}@example.test`;
    trackedEmails.add(disabledEmail);
    const disabledLogin = await ldapLogin(context.baseURL, {
      email: disabledEmail,
      password: 'disabled-policy-password'
    }, 403);
    assert.deepEqual(disabledLogin.data, { error: 'LDAP login is disabled' });
    assert.equal((await standardSession.client.get('/api/auth/me')).data.id, standardSession.data.user.id);
    await setPolicy(admin, enabledPolicy);
    await saveConfiguration(admin, plain);

    const resetIp = '198.51.100.241';
    trackedIps.add(resetIp);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      assertGenericLoginFailure(await ldapLogin(context.baseURL, {
        email: STANDARD_USER.email,
        password: 'wrong-reset-password'
      }, 401, { 'x-forwarded-for': resetIp }));
    }
    assert.equal(
      context.compose.psql(`SELECT failed_attempts FROM login_email_failures WHERE normalized_email = ${sqlLiteral(STANDARD_USER.email)}`),
      '2'
    );
    await ldapLogin(context.baseURL, STANDARD_USER, 200, { 'x-forwarded-for': resetIp });
    assert.equal(
      context.compose.psql(`SELECT count(*) FROM login_email_failures WHERE normalized_email = ${sqlLiteral(STANDARD_USER.email)}`),
      '0',
      'Successful LDAP login must clear the normalized-email failure window'
    );

    const limitedEmail = `${uniqueSlug(context, 'qa-ldap-email-limit')}@example.test`;
    const limitedIp = '198.51.100.242';
    trackedEmails.add(limitedEmail);
    trackedIps.add(limitedIp);
    for (let attempt = 0; attempt < 3; attempt += 1) {
      assertGenericLoginFailure(await ldapLogin(context.baseURL, {
        email: limitedEmail,
        password: 'wrong-email-limit-password'
      }, 401, { 'x-forwarded-for': limitedIp }));
    }
    const emailLimited = await ldapLogin(context.baseURL, {
      email: limitedEmail,
      password: 'wrong-email-limit-password'
    }, 429, { 'x-forwarded-for': limitedIp });
    assertRetryAfter(emailLimited);
    context.compose.psql(`
      UPDATE login_email_failures
      SET window_started_at = now() - interval '6 minutes'
      WHERE normalized_email = ${sqlLiteral(limitedEmail)}
    `);
    assertGenericLoginFailure(await ldapLogin(context.baseURL, {
      email: limitedEmail,
      password: 'wrong-after-expiry-password'
    }, 401, { 'x-forwarded-for': limitedIp }));
    assert.equal(
      context.compose.psql(`SELECT failed_attempts FROM login_email_failures WHERE normalized_email = ${sqlLiteral(limitedEmail)}`),
      '1',
      'Expired email windows must restart from one failure'
    );

    const ipLimited = '198.51.100.243';
    trackedIps.add(ipLimited);
    for (let attempt = 1; attempt <= 20; attempt += 1) {
      const email = `${uniqueSlug(context, `qa-ip-limit-${attempt}`)}@example.test`;
      trackedEmails.add(email);
      const response = await new ApiClient(context.baseURL).post('/api/auth/login', {
        email,
        password: 'wrong-ip-limit-password'
      }, { expectedStatus: 401, headers: { 'x-forwarded-for': ipLimited } });
      assert.deepEqual(response.data, { error: 'invalid credentials' });
    }
    const ipLimitedEmail = `${uniqueSlug(context, 'qa-ip-limit-final')}@example.test`;
    trackedEmails.add(ipLimitedEmail);
    const ipLimitResponse = await new ApiClient(context.baseURL).post('/api/auth/ldap/login', {
      email: ipLimitedEmail,
      password: 'wrong-ip-limit-password'
    }, { expectedStatus: 429, headers: { 'x-forwarded-for': ipLimited } });
    assertRetryAfter(ipLimitResponse);
    assert.equal(
      context.compose.psql(`SELECT attempts FROM login_ip_attempts WHERE source_ip = ${sqlLiteral(ipLimited)}::inet`),
      '21'
    );

    const logs = context.compose.logs();
    for (const credential of [
      STANDARD_USER.password,
      ESCAPING_USER.password,
      MAPPED_USER.password,
      NO_DISPLAY_USER.password,
      DUPLICATE_USER.password,
      'wrong-email-limit-password',
      'wrong-ip-limit-password'
    ]) {
      assert.equal(logs.includes(credential), false, 'Compose logs must not expose LDAP or login credentials');
    }
  } finally {
    const cleanRateState = () => {
      const emails = [...trackedEmails].map(sqlLiteral).join(', ');
      const ips = [...trackedIps].map((ip) => `${sqlLiteral(ip)}::inet`).join(', ');
      if (emails) context.compose.psql(`DELETE FROM login_email_failures WHERE normalized_email IN (${emails})`);
      if (ips) context.compose.psql(`DELETE FROM login_ip_attempts WHERE source_ip IN (${ips})`);
    };
    const authRestores = ldapSnapshot
      ? [
          ['restore LDAP configuration', () => restoreLdapConfiguration(context, admin, ldapSnapshot)],
          ['restore authentication policy', () => setPolicy(admin, policySnapshot)]
        ]
      : [
          ['restore authentication policy', () => setPolicy(admin, policySnapshot)],
          ['remove scenario LDAP configuration', () => restoreLdapConfiguration(context, admin, null)]
        ];
    await runCleanupSteps([
      ['clear scenario login throttle rows', cleanRateState],
      ...authRestores,
      [
        'verify restored LDAP state',
        async () => {
          assert.deepEqual((await admin.get('/api/admin/auth-policy')).data, policySnapshot);
          assert.deepEqual((await admin.get('/api/admin/ldap-config')).data, ldapSnapshot);
        }
      ]
    ]);
  }
}
