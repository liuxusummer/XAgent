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
  Brain,
  Shield,
  BarChart3,
  Activity,
} from 'lucide-react';
import { api } from '../api/client';
import type { ChatMetadata } from '../types';

export type PanelTab = 'agents' | 'skills' | 'memory' | 'system' | 'eval' | 'usage';

interface SidebarProps {
  onNewChat: () => void;
  onClearChat: () => void;
  currentSessionId: string;
  currentAgent: string | null;
  chats: ChatMetadata[];
  activeChatId: string;
  chatsLoading: boolean;
  onSelectChat: (chatId: string) => void;
  onDeleteChat: (chatId: string) => void;
  onOpenSettings: () => void;
  currentWorkspace: string;
  onWorkspaceChange: (ws: string) => void;
  onOpenPanel: (tab: PanelTab) => void;
  activeView: 'chat' | PanelTab | 'agent-detail';
}

export function Sidebar({
  onNewChat,
  onClearChat,
  currentAgent,
  chats,
  activeChatId,
  chatsLoading,
  onSelectChat,
  onDeleteChat,
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
          <button
            onClick={() => onOpenPanel('memory')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'memory'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="Memory"
          >
            <Brain className="w-4 h-4" />
          </button>
          <button
            onClick={() => onOpenPanel('system')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'system'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="System"
          >
            <Shield className="w-4 h-4" />
          </button>
          <button
            onClick={() => onOpenPanel('eval')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'eval'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="Eval"
          >
            <BarChart3 className="w-4 h-4" />
          </button>
          <button
            onClick={() => onOpenPanel('usage')}
            className={`flex items-center justify-center w-9 h-9 rounded-button transition-colors ${
              activeView === 'usage'
                ? 'bg-accent/10 text-accent'
                : 'text-text-muted hover:bg-bg-tertiary hover:text-text-secondary'
            }`}
            title="Usage"
          >
            <Activity className="w-4 h-4" />
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
        <button
          onClick={() => onOpenPanel('memory')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'memory'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <Brain className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">Memory</span>
        </button>
        <button
          onClick={() => onOpenPanel('system')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'system'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <Shield className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">System</span>
        </button>
        <button
          onClick={() => onOpenPanel('eval')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'eval'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <BarChart3 className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">Eval</span>
        </button>
        <button
          onClick={() => onOpenPanel('usage')}
          className={`flex items-center gap-2.5 w-full px-3 py-2 rounded-button text-sm transition-colors ${
            activeView === 'usage'
              ? 'bg-accent/10 text-accent'
              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
          }`}
        >
          <div className="w-7 h-7 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
            <Activity className="w-3.5 h-3.5 text-accent" />
          </div>
          <span className="font-medium">Usage</span>
        </button>
      </div>

      {/* Session */}
      <div className="px-3 py-2 border-t border-border/70">
        {currentAgent ? (
          <div className="space-y-2">
            <div className="flex items-center justify-between px-1">
              <div className="text-[11px] font-medium text-text-muted uppercase tracking-wider">
                {currentAgent} Chats
              </div>
              {chatsLoading && <span className="text-[11px] text-text-muted">Loading</span>}
            </div>
            <div className="max-h-56 overflow-y-auto space-y-1">
              {chats.length === 0 ? (
                <div className="px-3 py-3 text-xs text-text-muted rounded-button bg-bg-tertiary/50">
                  No saved chats yet.
                </div>
              ) : (
                chats.map((chat) => (
                  <div
                    key={chat.chat_id}
                    className={`group flex items-start gap-2 w-full px-3 py-2 rounded-button transition-colors text-left ${
                      activeChatId === chat.chat_id
                        ? 'bg-accent/10 text-accent'
                        : 'hover:bg-bg-tertiary text-text-secondary'
                    }`}
                  >
                    <button
                      type="button"
                      onClick={() => onSelectChat(chat.chat_id)}
                      className="flex min-w-0 flex-1 items-start gap-2 text-left"
                    >
                      <MessageSquare className="w-4 h-4 mt-0.5 shrink-0" />
                      <span className="min-w-0 flex-1">
                        <span className="block text-sm font-medium truncate">{chat.title || 'New Chat'}</span>
                        <span className="block text-[11px] text-text-muted truncate">
                          {chat.last_message_preview || formatChatTime(chat.updated_at)}
                        </span>
                      </span>
                    </button>
                    <button
                      type="button"
                      onClick={() => onDeleteChat(chat.chat_id)}
                      className="opacity-0 group-hover:opacity-100 text-text-muted hover:text-status-error transition-opacity"
                      title="Delete chat"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        ) : (
          <button
            onClick={onClearChat}
            className="flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors group"
          >
            <MessageSquare className="w-4 h-4 text-text-muted group-hover:text-text-secondary" />
            <span className="text-sm text-text-secondary truncate flex-1 text-left">Current Chat</span>
            <Trash2 className="w-3.5 h-3.5 text-text-muted opacity-0 group-hover:opacity-100 transition-opacity" />
          </button>
        )}
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

function formatChatTime(seconds: number): string {
  if (!seconds) return '';
  return new Date(seconds * 1000).toLocaleDateString();
}
