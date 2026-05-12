import { useState } from 'react';
import { Bot, MessageSquare, Plus, Trash2, ChevronLeft, ChevronRight, Settings } from 'lucide-react';

interface SidebarProps {
  onNewChat: () => void;
  onClearChat: () => void;
  currentSessionId: string;
  onOpenSettings: () => void;
}

export function Sidebar({ onNewChat, onClearChat, onOpenSettings }: SidebarProps) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`flex flex-col bg-bg-secondary border-r border-border transition-all duration-300 ${
        collapsed ? 'w-16' : 'w-64'
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-border">
        {!collapsed && (
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-accent/20 flex items-center justify-center">
              <Bot className="w-5 h-5 text-accent" />
            </div>
            <span className="font-semibold text-text-primary">XAgent</span>
          </div>
        )}
        {collapsed && (
          <div className="w-8 h-8 rounded-lg bg-accent/20 flex items-center justify-center mx-auto">
            <Bot className="w-5 h-5 text-accent" />
          </div>
        )}
      </div>

      {/* New Chat Button */}
      <div className="p-3">
        <button
          onClick={onNewChat}
          className={`flex items-center gap-2 w-full px-3 py-2.5 rounded-button bg-accent/10 hover:bg-accent/20 text-accent transition-colors ${
            collapsed ? 'justify-center' : ''
          }`}
        >
          <Plus className="w-4 h-4" />
          {!collapsed && <span className="text-sm font-medium">New Chat</span>}
        </button>
      </div>

      {/* Session List */}
      <div className="flex-1 overflow-y-auto px-3">
        {!collapsed && (
          <div className="text-xs font-medium text-text-muted uppercase tracking-wider mb-2 px-2">
            Current Session
          </div>
        )}
        <button
          onClick={onClearChat}
          className={`flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors group ${
            collapsed ? 'justify-center' : ''
          }`}
        >
          <MessageSquare className="w-4 h-4 text-text-muted group-hover:text-text-secondary" />
          {!collapsed && (
            <>
              <span className="text-sm text-text-secondary truncate flex-1 text-left">Current Chat</span>
              <Trash2 className="w-3.5 h-3.5 text-text-muted opacity-0 group-hover:opacity-100 transition-opacity" />
            </>
          )}
        </button>
      </div>

      {/* Footer */}
      <div className="p-3 border-t border-border">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className={`flex items-center gap-2 w-full px-3 py-2 rounded-button hover:bg-bg-tertiary transition-colors text-text-muted ${
            collapsed ? 'justify-center' : ''
          }`}
        >
          {collapsed ? (
            <ChevronRight className="w-4 h-4" />
          ) : (
            <>
              <ChevronLeft className="w-4 h-4" />
              <span className="text-sm">Collapse</span>
            </>
          )}
        </button>

        {!collapsed && (
          <button
            onClick={onOpenSettings}
            className="flex items-center gap-2 w-full px-3 py-2 mt-1 rounded-button hover:bg-bg-tertiary transition-colors text-text-muted"
          >
            <Settings className="w-4 h-4" />
            <span className="text-sm">Settings</span>
          </button>
        )}
      </div>
    </aside>
  );
}
