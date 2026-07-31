import { BookOpen, Check, Copy, ExternalLink, ShieldCheck } from 'lucide-react';
import { useState } from 'react';
import { useI18n } from './i18n';

const serverAccessExample = `export async function issueAgentHubAccess(user, clientInstanceId) {
  const basic = Buffer.from(
    \`\${process.env.AGENT_HUB_CLIENT_ID}:\${process.env.AGENT_HUB_CLIENT_SECRET}\`,
  ).toString("base64");

  const response = await fetch(\`\${process.env.AGENT_HUB_URL}/api/client/access\`, {
    method: "POST",
    headers: {
      Authorization: \`Basic \${basic}\`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      agent_id: process.env.AGENT_HUB_AGENT_ID,
      client_instance_id: clientInstanceId,
      external_user_id: user.id,
      tenant_id: user.tenantId,
      display_name: user.displayName,
      email: user.email,
      attributes: {},
      client_tools: [],
    }),
  });
  if (!response.ok) throw new Error(\`Agent Hub access failed: \${response.status}\`);
  return response.json();
}`;

const browserSdkExample = `import { connect, type SessionEvent } from "@agent-hub/client";

const client = await connect({
  baseUrl: "https://hub.example.com",
  authorize: async ({ clientInstanceId, signal }) => {
    const response = await fetch("/api/agent-hub/access", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ clientInstanceId }),
      signal,
    });
    if (!response.ok) throw new Error(\`authorize failed: \${response.status}\`);
    return response.json();
  },
});

const session = client.sessions.draft();
await session.send("Hello", {
  clientMessageKey: \`message:\${crypto.randomUUID()}\`,
});

const subscription = session.subscribe((event: SessionEvent) => {
  if (event.type === "message" || event.type === "assistant") {
    console.log(event.content);
  } else if (event.type === "error") {
    console.error(event.code, event.message);
  }
});`;

const anonymousExample = `import { connectAnonymous } from "@agent-hub/client";

const client = await connectAnonymous({
  baseUrl: "https://hub.example.com",
  clientId: "replace-with-public-client-id",
});

const session = client.currentSession() ?? client.draft();
await session.send("Hello", {
  clientMessageKey: \`message:\${crypto.randomUUID()}\`,
});`;

const widgetExample = `<iframe
  src="https://hub.example.com/widget?app=replace-with-public-client-id"
  title="AI assistant"
  width="420"
  height="720"
></iframe>`;

const serverApiExample = `const basic = Buffer.from(
  \`\${process.env.AGENT_HUB_CLIENT_ID}:\${process.env.AGENT_HUB_CLIENT_SECRET}\`,
).toString("base64");
const form = new URLSearchParams({
  grant_type: "client_credentials",
  scope: \`agent:\${process.env.AGENT_HUB_AGENT_ID}\`,
});

const response = await fetch(\`\${process.env.AGENT_HUB_URL}/api/oauth/token\`, {
  method: "POST",
  headers: {
    Authorization: \`Basic \${basic}\`,
    "Content-Type": "application/x-www-form-urlencoded",
  },
  body: form,
});
if (!response.ok) throw new Error(\`OAuth failed: \${response.status}\`);
const token = await response.json();`;

const clientToolExample = `client.registerTool("create_ticket", async (input, context) => {
  const response = await fetch("/api/tickets", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": context.toolCallId,
    },
    body: JSON.stringify(input),
    signal: context.signal,
  });
  if (!response.ok) throw new Error("Ticket service failed");
  return response.json();
});`;

function CodeExample({ code, label }: { code: string; label: string }) {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  }

  return <div className="usage-code-example">
    <div className="usage-code-toolbar"><span>{label}</span><button className="icon-button" type="button" aria-label={copied ? t('copied') : t('copyCode')} title={copied ? t('copied') : t('copyCode')} onClick={copy}>{copied ? <Check size={16} /> : <Copy size={16} />}</button></div>
    <pre tabIndex={0}><code>{code}</code></pre>
  </div>;
}

export function UsageDocsPage() {
  const { t } = useI18n();
  return <div className="docs-page usage-docs-page">
    <header className="docs-header">
      <div>
        <div className="section-title"><BookOpen size={20} /> {t('usageGuide')}</div>
        <h1>{t('thirdPartyIntegrationGuide')}</h1>
        <p>{t('thirdPartyIntegrationSubtitle')}</p>
      </div>
      <a className="secondary link-button" href="/docs">{t('openApiReference')} <ExternalLink size={15} /></a>
    </header>

    <section className="docs-section" id="integration-mode">
      <h2>{t('chooseIntegrationMode')}</h2>
      <p>{t('chooseIntegrationModeHelp')}</p>
      <div className="usage-mode-table" role="table" aria-label={t('integrationModes')}>
        <div className="usage-mode-row usage-mode-header" role="row"><span role="columnheader">{t('integrationMode')}</span><span role="columnheader">{t('bestFor')}</span><span role="columnheader">{t('usageCapabilities')}</span></div>
        <div className="usage-mode-row" role="row"><strong role="cell">{t('authenticatedBrowser')}</strong><span role="cell">{t('authenticatedBrowserFor')}</span><span role="cell">{t('authenticatedBrowserCapabilities')}</span></div>
        <div className="usage-mode-row" role="row"><strong role="cell">{t('anonymousBrowser')}</strong><span role="cell">{t('anonymousBrowserFor')}</span><span role="cell">{t('anonymousBrowserCapabilities')}</span></div>
        <div className="usage-mode-row" role="row"><strong role="cell">{t('serverApi')}</strong><span role="cell">{t('serverApiFor')}</span><span role="cell">{t('serverApiCapabilities')}</span></div>
      </div>
    </section>

    <section className="docs-section" id="prepare">
      <h2>{t('prepareIntegration')}</h2>
      <ol className="usage-steps">
        <li>{t('prepareExternalPlatform')}</li>
        <li>{t('prepareAgent')}</li>
        <li>{t('prepareApp')}</li>
        <li>{t('preparePolicies')}</li>
        <li>{t('prepareCredentials')}</li>
      </ol>
      <div className="usage-callout"><ShieldCheck size={18} /><p>{t('clientSecretWarning')}</p></div>
    </section>

    <section className="docs-section" id="authenticated">
      <h2>{t('authenticatedIntegration')}</h2>
      <p>{t('authenticatedIntegrationHelp')}</p>
      <h3>{t('backendCredentialExchange')}</h3>
      <p>{t('backendCredentialExchangeHelp')}</p>
      <CodeExample label="server.ts" code={serverAccessExample} />
      <h3>{t('browserSdk')}</h3>
      <p>{t('browserSdkInstallHelp')}</p>
      <CodeExample label="browser.ts" code={browserSdkExample} />
      <p className="usage-note">{t('draftSessionHelp')}</p>
    </section>

    <section className="docs-section" id="anonymous">
      <h2>{t('anonymousIntegration')}</h2>
      <p>{t('anonymousIntegrationHelp')}</p>
      <CodeExample label="browser.ts" code={anonymousExample} />
      <h3>{t('hostedWidget')}</h3>
      <p>{t('hostedWidgetHelp')}</p>
      <CodeExample label="HTML" code={widgetExample} />
    </section>

    <section className="docs-section" id="history-tools">
      <h2>{t('historyAndTools')}</h2>
      <h3>{t('conversationHistory')}</h3>
      <p>{t('conversationHistoryHelp')}</p>
      <h3>{t('usageClientTools')}</h3>
      <p>{t('clientToolsHelp')}</p>
      <CodeExample label="browser.ts" code={clientToolExample} />
      <p className="usage-note">{t('clientToolIdempotency')}</p>
    </section>

    <section className="docs-section" id="server-api">
      <h2>{t('serverApiIntegration')}</h2>
      <p>{t('serverApiIntegrationHelp')}</p>
      <CodeExample label="server.mjs" code={serverApiExample} />
      <ol className="usage-steps">
        <li>{t('serverApiStepToken')}</li>
        <li>{t('serverApiStepSession')}</li>
        <li>{t('serverApiStepMessage')}</li>
        <li>{t('serverApiStepStop')}</li>
      </ol>
      <p className="usage-note">{t('oauthStateResponsibility')}</p>
    </section>

    <section className="docs-section" id="operations">
      <h2>{t('lifecycleAndSecurity')}</h2>
      <ul className="usage-checklist">
        <li>{t('credentialLifecycle')}</li>
        <li>{t('messageIdempotency')}</li>
        <li>{t('sseRecovery')}</li>
        <li>{t('singleActiveTurn')}</li>
        <li>{t('originSecurity')}</li>
        <li>{t('disposeResources')}</li>
      </ul>
      <p>{t('usageDocsApiReferenceHelp')} <a href="/docs">{t('apiDocs')}</a>.</p>
    </section>
  </div>;
}
