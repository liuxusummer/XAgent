import { useState, useCallback } from 'react';
import { X, Save, RotateCcw, FileJson, Folder, Eye, EyeOff } from 'lucide-react';
import {
  DEFAULT_CONFIG,
  loadConfig,
  saveConfigToStorage,
  type AgentConfig,
} from '../config/agentConfig';

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (config: AgentConfig) => void;
  initialConfig?: AgentConfig;
}

export function SettingsModal({ isOpen, onClose, onSave, initialConfig }: SettingsModalProps) {
  const [config, setConfig] = useState<AgentConfig>(() => initialConfig || loadConfig());
  const [showKey, setShowKey] = useState(false);

  const handleChange = useCallback((field: keyof AgentConfig, value: string) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  }, []);

  const handleReset = useCallback(() => {
    setConfig(DEFAULT_CONFIG);
  }, []);

  const handleSave = useCallback(() => {
    saveConfigToStorage(config);
    onSave(config);
    onClose();
  }, [config, onSave, onClose]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative w-full max-w-lg mx-4 bg-bg-secondary border border-border rounded-card shadow-2xl animate-fade-in">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border">
          <div className="flex items-center gap-2">
            <FileJson className="w-5 h-5 text-accent" />
            <h2 className="text-lg font-semibold text-text-primary">Settings</h2>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-button hover:bg-bg-tertiary text-text-muted hover:text-text-secondary transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-5 space-y-5">
          {/* Config Path */}
          <div className="space-y-1.5">
            <label className="flex items-center gap-1.5 text-sm font-medium text-text-secondary">
              <FileJson className="w-3.5 h-3.5 text-text-muted" />
              Config Path
            </label>
            <div className="relative">
              <input
                type="text"
                value={config.configPath}
                onChange={e => handleChange('configPath', e.target.value)}
                placeholder="config.json"
                className="w-full px-3 py-2.5 bg-bg-tertiary border border-border rounded-input text-sm text-text-primary placeholder-text-muted outline-none focus:border-accent/50 transition-colors"
              />
            </div>
            <p className="text-xs text-text-muted">
              Path to the agent configuration JSON file.
            </p>
          </div>

          {/* Observability Config */}
          <div className="space-y-1.5">
            <label className="flex items-center gap-1.5 text-sm font-medium text-text-secondary">
              <Eye className="w-3.5 h-3.5 text-text-muted" />
              Observability Config
            </label>
            <div className="relative">
              <input
                type={showKey ? 'text' : 'password'}
                value={config.observabilityConfigPath}
                onChange={e => handleChange('observabilityConfigPath', e.target.value)}
                placeholder="observability.example.json"
                className="w-full px-3 py-2.5 pr-10 bg-bg-tertiary border border-border rounded-input text-sm text-text-primary placeholder-text-muted outline-none focus:border-accent/50 transition-colors"
              />
              <button
                type="button"
                onClick={() => setShowKey(!showKey)}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 p-1 text-text-muted hover:text-text-secondary transition-colors"
              >
                {showKey ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
              </button>
            </div>
            <p className="text-xs text-text-muted">
              Optional observability configuration file path.
            </p>
          </div>

          {/* Workspace Dir */}
          <div className="space-y-1.5">
            <label className="flex items-center gap-1.5 text-sm font-medium text-text-secondary">
              <Folder className="w-3.5 h-3.5 text-text-muted" />
              Workspace Directory
            </label>
            <div className="relative">
              <input
                type="text"
                value={config.workspaceDir}
                onChange={e => handleChange('workspaceDir', e.target.value)}
                placeholder="Leave empty for default"
                className="w-full px-3 py-2.5 bg-bg-tertiary border border-border rounded-input text-sm text-text-primary placeholder-text-muted outline-none focus:border-accent/50 transition-colors"
              />
            </div>
            <p className="text-xs text-text-muted">
              Working directory for agent file operations.
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-border">
          <button
            onClick={handleReset}
            className="flex items-center gap-1.5 px-3 py-2 rounded-button text-sm text-text-muted hover:text-text-secondary hover:bg-bg-tertiary transition-colors"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset
          </button>

          <div className="flex items-center gap-2">
            <button
              onClick={onClose}
              className="px-4 py-2 rounded-button text-sm text-text-secondary hover:bg-bg-tertiary transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              className="flex items-center gap-1.5 px-4 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors"
            >
              <Save className="w-3.5 h-3.5" />
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
