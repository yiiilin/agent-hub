import type { FullConfig } from '@playwright/test';
import {
  assertComposeFrontendTarget,
  e2eComposeProject,
  isPlaywrightDiscovery
} from './e2e-compose';

export default function globalSetup(config: FullConfig) {
  if (isPlaywrightDiscovery()) return;

  const baseURLs = new Set(config.projects.map((project) => project.use.baseURL).filter((value): value is string => typeof value === 'string'));
  if (baseURLs.size !== 1) {
    throw new Error(`Expected one Playwright baseURL across all projects, found ${baseURLs.size}.`);
  }
  assertComposeFrontendTarget(e2eComposeProject(), [...baseURLs][0]);
}
