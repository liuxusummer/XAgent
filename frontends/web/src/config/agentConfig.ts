export interface AgentConfig {
  configPath: string;
  observabilityConfigPath: string;
  workspaceDir: string;
}

export const DEFAULT_CONFIG: AgentConfig = {
  configPath: 'config.json',
  observabilityConfigPath: '',
  workspaceDir: '',
};

const STORAGE_KEY = 'xagent-config';

function loadStoredConfig(): AgentConfig | undefined {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch {
    // Invalid or unavailable browser storage falls back to defaults.
  }
  return undefined;
}

export function saveConfigToStorage(config: AgentConfig): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(config));
}

export function loadConfig(): AgentConfig {
  return loadStoredConfig() || DEFAULT_CONFIG;
}
