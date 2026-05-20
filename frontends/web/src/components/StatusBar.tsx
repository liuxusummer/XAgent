import { Bot, Loader2, AlertCircle, Pause, Radio, Sun, Moon, Sparkles } from 'lucide-react';
import type { AgentStatus } from '../types';
import { useTheme } from '../hooks/useTheme.tsx';

interface StatusBarProps {
  status: AgentStatus;
  activeAgent?: string | null;
  chatTitle?: string;
}

export function StatusBar({ status, activeAgent, chatTitle }: StatusBarProps) {
  const { theme, toggleTheme } = useTheme();
  const getStatusConfig = () => {
    switch (status.state) {
      case 'thinking':
        return {
          icon: <Loader2 className="w-4 h-4 animate-spin" />,
          text: 'Thinking...',
          color: 'text-accent',
          bgColor: 'bg-accent/10',
          pulse: true,
        };
      case 'executing':
        return {
          icon: <Radio className="w-4 h-4" />,
          text: status.currentTool ? `Executing ${status.currentTool}...` : 'Executing...',
          color: 'text-status-success',
          bgColor: 'bg-status-success/10',
          pulse: true,
        };
      case 'waiting_for_user':
        return {
          icon: <Pause className="w-4 h-4" />,
          text: 'Waiting for your reply...',
          color: 'text-status-warning',
          bgColor: 'bg-status-warning/10',
          pulse: false,
        };
      case 'error':
        return {
          icon: <AlertCircle className="w-4 h-4" />,
          text: 'Error occurred',
          color: 'text-status-error',
          bgColor: 'bg-status-error/10',
          pulse: false,
        };
      default:
        return {
          icon: <Bot className="w-4 h-4" />,
          text: 'Ready',
          color: 'text-text-muted',
          bgColor: 'bg-bg-tertiary',
          pulse: false,
        };
    }
  };

  const config = getStatusConfig();

  return (
    <div className="flex items-center gap-3 px-4 py-2.5 bg-bg-secondary border-b border-border">
      <div
        className={`flex items-center gap-2 px-3 py-1.5 rounded-full ${config.bgColor} ${
          config.pulse ? 'status-pulse' : ''
        }`}
      >
        <span className={config.color}>{config.icon}</span>
        <span className={`text-sm font-medium ${config.color}`}>{config.text}</span>
      </div>

      <div className="flex items-center gap-3 ml-auto">
        {activeAgent && (
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-accent/10 border border-accent/20">
            <Sparkles className="w-3 h-3 text-accent" />
            <span className="text-xs font-medium text-accent">{activeAgent}</span>
          </div>
        )}

        {activeAgent && chatTitle && (
          <div className="max-w-56 truncate text-xs text-text-muted">
            {chatTitle}
          </div>
        )}

        {status.currentTurn && status.maxTurns && (
          <div className="text-xs text-text-muted">
            Turn {status.currentTurn} / {status.maxTurns}
          </div>
        )}

        <button
          onClick={toggleTheme}
          className="p-1.5 rounded-button hover:bg-bg-tertiary text-text-muted hover:text-text-secondary transition-colors"
          title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
        </button>
      </div>
    </div>
  );
}
