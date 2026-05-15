import { useState, useEffect, useCallback } from 'react';
import {
  Bot,
  MessageSquare,
  Plus,
  Trash2,
  ChevronLeft,
  ChevronRight,
  Settings,
  FolderOpen,
  User,
  Zap,
} from 'lucide-react';
import { api } from '../api/client';

export type PanelTab = 'agents' | 'skills';

interface SidebarProps {
  onNewChat: () => void;
  onClearChat: () => void;
  currentSessionId: string;
  onOpenSettings: () => void;
  currentWorkspace: string;
  onWorkspaceChange: (ws: string) => void;
  onOpenPanel: (tab: PanelTab) => void;
  activeView: 'chat' | PanelTab;
}

export function Sidebar({
  onNewChat,
  onClearChat,
  onOpenSettings,
  currentWorkspace,
  onWorkspaceChange,
  onOpenPanel,
  activeView,
}: SidebarProps) {
  const [collapsed, setCollapsed] = useState(false);
  const [workspaces, setWorkspaces] = useState<string[]>([]);

  useEffect(() => {
    api.listWorkspaces().then(res => {
      if (res.success && res.data) setWorkspaces(res.data);
    });
  }, []);

  const handleWorkspaceChange = useCallback(
    (ws: string) => {
      onWorkspaceChange(ws);
    },
    [onWorkspaceChange]
  );

  if (collapsed) {
    return (
      <aside className="flex flex-col w-16 bg-bg-secondary border-r border-border transition-all duration-300">
        <div className="flex items-center justify-center p-4 border-b border-border">
          <div className="w-8 h-8 rounded-lg bg-accent/20 flex items-center justify-center">
            <Bot className="w-5 h-5 text-accent" />
          </div>
        </div>
        <div className="p-3">
          <button
            onClick={onNewChat}
            className="flex items-center justify-center w-full px-3 py-2.5 rounded-button bg-accent/10 hover:bg-accent/20 text-accent transition-colors"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>
        <div className="flex-1 flex flex-col items-center py-2 gap-2">
          <button
            onClick={() => onOpenPanel('agents')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'agents'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="Agents"
          >
            <User className="w-4 h-4" />
          </button>
          <button
            onClick={() => onOpenPanel('skills')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'skills'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="Skills"
          >
            <Zap className="w-4 h-4" />
          </button>
        </div>
        <div className="p-3 border-t border-border">
          <button
            onClick={() => setCollapsed(false)}
            className="flex items-center justify-center w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors text-text-muted"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </aside>
    );
  }

  return (
    <aside className="flex flex-col w-64 bg-bg-secondary border-r border-border transition-all duration-300">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-accent/20 flex items-center justify-center">
            <Bot className="w-5 h-5 text-accent" />
          </div>
          <span className="font-semibold text-text-primary">XAgent</span>
        </div>
      </div>

      {/* Workspace Selector */}
      <div className="px-3 pt-3 pb-1">
        <label className="flex items-center gap-1.5 text-[11px] font-medium text-text-muted uppercase tracking-wider mb-1.5 px-1">
          <FolderOpen className="w-3 h-3" />
          Workspace
        </label>
        <select
          value={currentWorkspace}
          onChange={e => handleWorkspaceChange(e.target.value)}
          className="w-full px-2.5 py-1.5 bg-bg-tertiary border border-border rounded-button text-sm text-text-primary outline-none focus:border-accent/50 transition-colors appearance-none cursor-pointer"
        >
          {workspaces.map(ws => (
            <option key={ws} value={ws}>
              {ws.replace(/\.ws$/, '')}
            </option>
          ))}
          {workspaces.length === 0 && (
            <option value="default.ws">default</option>
          )}
        </select>
      </div>

      {/* New Chat */}
      <div className="px-3 pt-2 pb-1">
        <button
          onClick={onNewChat}
          className="flex items-center gap-2 w-full px-3 py-2 rounded-button bg-accent/10 hover:bg-accent/20 text-accent transition-colors"
        >
          <Plus className="w-4 h-4" />
          <span className="text-sm font-medium">New Chat</span>
        </button>
      </div>

      {/* Config Buttons */}
      <div className="flex-1 overflow-y-auto px-3 py-2 space-y-1">
        <div className="text-[11px] font-medium text-text-muted uppercase tracking-wider mb-1.5 px-1">
          Configuration
        </div>
        <button
          onClick={() => onOpenPanel('agents')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'agents'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <User className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">Agents</span>
        </button>
        <button
          onClick={() => onOpenPanel('skills')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'skills'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <Zap className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">Skills</span>
        </button>
      </div>

      {/* Session */}
      <div className="px-3 py-1">
        <button
          onClick={onClearChat}
          className="flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors group"
        >
          <MessageSquare className="w-4 h-4 text-text-muted group-hover:text-text-secondary" />
          <span className="text-sm text-text-secondary truncate flex-1 text-left">Current Chat</span>
          <Trash2 className="w-3.5 h-3.5 text-text-muted opacity-0 group-hover:opacity-100 transition-opacity" />
        </button>
      </div>

      {/* Footer */}
      <div className="p-3 border-t border-border space-y-1">
        <button
          onClick={() => setCollapsed(true)}
          className="flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors text-text-muted"
        >
          <ChevronLeft className="w-4 h-4" />
          <span className="text-sm">Collapse</span>
        </button>
        <button
          onClick={onOpenSettings}
          className="flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors text-text-muted"
        >
          <Settings className="w-4 h-4" />
          <span className="text-sm">Settings</span>
        </button>
      </div>
    </aside>
  );
}
