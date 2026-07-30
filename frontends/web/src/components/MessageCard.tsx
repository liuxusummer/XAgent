import { useState } from 'react';
import ReactMarkdown, { type Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { User, Bot, Wrench, ChevronDown, ChevronUp, Copy, Check, Terminal, Lightbulb, Loader2 } from 'lucide-react';
import type { Message, ToolCall } from '../types';

interface MessageCardProps {
  message: Message;
}

function formatCompletedAt(value: number): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (part: number) => part.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function ToolCallCard({ toolCall }: { toolCall: ToolCall }) {
  const [expanded, setExpanded] = useState(false);

  const statusColors = {
    pending: 'text-status-warning bg-status-warning/10 border-status-warning/20',
    running: 'text-accent bg-accent/10 border-accent/20',
    success: 'text-status-success bg-status-success/10 border-status-success/20',
    error: 'text-status-error bg-status-error/10 border-status-error/20',
  };

  return (
    <div className={`mt-2 rounded-lg border ${statusColors[toolCall.status]} overflow-hidden`}>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 hover:bg-white/5 transition-colors"
      >
        <Wrench className="w-3.5 h-3.5" />
        <span className="text-sm font-medium">{toolCall.name}</span>
        <span className="text-xs opacity-60 ml-auto">{toolCall.status}</span>
        {expanded ? (
          <ChevronUp className="w-3.5 h-3.5" />
        ) : (
          <ChevronDown className="w-3.5 h-3.5" />
        )}
      </button>

      {expanded && (
        <div className="px-3 pb-3 border-t border-white/10">
          <div className="mt-2">
            <div className="text-xs text-text-muted mb-1">Arguments</div>
            <pre className="text-xs bg-black/20 rounded p-2 overflow-x-auto font-mono">
              {JSON.stringify(toolCall.arguments, null, 2)}
            </pre>
          </div>

          {toolCall.result !== undefined && (
            <div className="mt-2">
              <div className="text-xs text-text-muted mb-1">Result</div>
              <pre className="text-xs bg-black/20 rounded p-2 overflow-x-auto font-mono">
                {typeof toolCall.result === 'string'
                  ? toolCall.result
                  : JSON.stringify(toolCall.result, null, 2)}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function CodeBlock({ code, language }: { code: string; language?: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group mt-2">
      <div className="flex items-center justify-between px-3 py-1.5 bg-bg-tertiary rounded-t-lg border border-border border-b-0">
        <div className="flex items-center gap-2">
          <Terminal className="w-3.5 h-3.5 text-text-muted" />
          <span className="text-xs text-text-muted">{language || 'code'}</span>
        </div>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1 text-xs text-text-muted hover:text-text-secondary transition-colors"
        >
          {copied ? (
            <>
              <Check className="w-3.5 h-3.5" />
              <span>Copied</span>
            </>
          ) : (
            <>
              <Copy className="w-3.5 h-3.5" />
              <span>Copy</span>
            </>
          )}
        </button>
      </div>
      <pre className="bg-bg-tertiary rounded-b-lg border border-border border-t-0 p-3 overflow-x-auto">
        <code className="text-sm font-mono text-text-secondary">{code}</code>
      </pre>
    </div>
  );
}

const markdownComponents: Components = {
  code({ className, children, ...props }) {
    const match = /language-(\w+)/.exec(className || '');
    const language = match ? match[1] : undefined;
    const code = children == null ? '' : String(children).replace(/\n$/, '');
    const isBlock = !!className || code.includes('\n');

    if (!isBlock) {
      return (
        <code className="px-1.5 py-0.5 rounded bg-bg-tertiary text-sm font-mono text-text-secondary" {...props}>
          {children}
        </code>
      );
    }

    return <CodeBlock code={code} language={language} />;
  },
  pre({ children }) {
    return <>{children}</>;
  },
  h1({ children }) {
    return <h1 className="text-xl font-bold text-text-primary mt-4 mb-2">{children}</h1>;
  },
  h2({ children }) {
    return <h2 className="text-lg font-semibold text-text-primary mt-3 mb-2">{children}</h2>;
  },
  h3({ children }) {
    return <h3 className="text-base font-semibold text-text-primary mt-3 mb-1.5">{children}</h3>;
  },
  p({ children }) {
    return <p className="text-sm leading-relaxed text-text-secondary mb-2 last:mb-0">{children}</p>;
  },
  ul({ children }) {
    return <ul className="list-disc list-inside text-sm text-text-secondary mb-2 space-y-1">{children}</ul>;
  },
  ol({ children }) {
    return <ol className="list-decimal list-inside text-sm text-text-secondary mb-2 space-y-1">{children}</ol>;
  },
  li({ children }) {
    return <li className="text-sm text-text-secondary">{children}</li>;
  },
  strong({ children }) {
    return <strong className="font-semibold text-text-primary">{children}</strong>;
  },
  em({ children }) {
    return <em className="italic text-text-secondary">{children}</em>;
  },
  blockquote({ children }) {
    return (
      <blockquote className="border-l-2 border-accent/40 pl-3 my-2 text-sm text-text-muted italic">
        {children}
      </blockquote>
    );
  },
  a({ children, href }) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
        {children}
      </a>
    );
  },
  hr() {
    return <hr className="border-border my-3" />;
  },
  table({ children }) {
    return (
      <div className="overflow-x-auto my-2">
        <table className="w-full text-sm text-text-secondary border-collapse">{children}</table>
      </div>
    );
  },
  thead({ children }) {
    return <thead className="bg-bg-tertiary">{children}</thead>;
  },
  th({ children }) {
    return <th className="px-3 py-2 text-left text-xs font-semibold text-text-primary border border-border">{children}</th>;
  },
  td({ children }) {
    return <td className="px-3 py-2 text-sm text-text-secondary border border-border">{children}</td>;
  },
};

export function MessageCard({ message }: MessageCardProps) {
  const isUser = message.role === 'user';
  const isSystem = message.role === 'system';
  const isStreaming = message.status === 'streaming';
  const hasContent = message.content.trim().length > 0;
  const hasThinking = !!message.thinking && message.thinking.trim().length > 0;
  const hasTools = message.toolCalls && message.toolCalls.length > 0;
  const hasAnyContent = hasContent || hasThinking || hasTools || isStreaming;
  const completedAt = message.metadata?.completedAt ? formatCompletedAt(message.metadata.completedAt) : '';

  if (isSystem) {
    return (
      <div className="flex justify-center my-3">
        <div className="px-4 py-2 rounded-full bg-bg-tertiary border border-border text-xs text-text-muted max-w-[80%] text-center">
          {message.content}
        </div>
      </div>
    );
  }

  return (
    <div className={`flex gap-4 mb-6 message-enter ${isUser ? 'flex-row-reverse' : ''}`}>
      {/* Avatar */}
      <div
        className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
          isUser
            ? 'bg-accent/20'
            : 'bg-gradient-to-br from-accent/30 to-accent/10'
        }`}
      >
        {isUser ? (
          <User className="w-4 h-4 text-accent" />
        ) : (
          <Bot className="w-4 h-4 text-accent" />
        )}
      </div>

      {/* Content */}
      <div className={`flex-1 max-w-[85%] ${isUser ? 'text-right' : ''}`}>
        {isUser ? (
          <div
            className="inline-block text-left px-4 py-3 rounded-card bg-accent/15 text-text-primary"
          >
            <div className="text-sm leading-relaxed whitespace-pre-wrap">
              {message.content}
            </div>
          </div>
        ) : (
          hasAnyContent && (
            <div
              className="block text-left px-4 py-3 rounded-card bg-bg-secondary border border-border text-text-primary max-w-full break-words"
            >
              {/* Thinking */}
              {hasThinking && (
                <ThinkingBlock thinking={message.thinking!} isStreaming={isStreaming} className="mb-2" />
              )}

              {/* Tool Calls */}
              {hasTools && (
                <div className={hasThinking ? 'mb-2' : ''}>
                  {message.toolCalls!.map(toolCall => (
                    <ToolCallCard key={toolCall.id} toolCall={toolCall} />
                  ))}
                </div>
              )}

              {hasContent && (
                <div className={`prose prose-sm max-w-full dark:prose-invert ${isStreaming ? 'typing-cursor' : ''}`}>
                  <ReactMarkdown
                    remarkPlugins={[remarkGfm]}
                    components={markdownComponents}
                  >
                    {message.content}
                  </ReactMarkdown>
                </div>
              )}

              {!hasContent && isStreaming && (
                <div className="flex items-center gap-2 text-sm text-text-muted py-1">
                  <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  <span>Thinking...</span>
                </div>
              )}
            </div>
          )
        )}

        {/* Metadata */}
        {completedAt && (
          <div className="mt-1 text-xs text-text-muted">
            Completed: {completedAt}
          </div>
        )}
      </div>
    </div>
  );
}

function ThinkingBlock({
  thinking,
  isStreaming,
  className = '',
}: {
  thinking: string;
  isStreaming: boolean;
  className?: string;
}) {
  const [expanded, setExpanded] = useState(isStreaming);

  return (
    <div className={`rounded-lg border border-accent/20 overflow-hidden ${className}`}>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-2 w-full px-3 py-2 bg-accent/5 hover:bg-accent/10 transition-colors"
      >
        <Lightbulb className="w-3.5 h-3.5 text-accent" />
        <span className="text-xs font-medium text-accent">Thinking</span>
        {isStreaming && (
          <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
        )}
        <span className="text-xs text-text-muted ml-auto">
          {expanded ? 'Hide' : 'Show'}
        </span>
        {expanded ? (
          <ChevronUp className="w-3.5 h-3.5 text-text-muted" />
        ) : (
          <ChevronDown className="w-3.5 h-3.5 text-text-muted" />
        )}
      </button>

      {expanded && (
        <div className="px-3 py-2 bg-bg-tertiary/50 border-t border-accent/10">
          <pre className="text-xs text-text-muted whitespace-pre-wrap font-mono leading-relaxed">
            {thinking}
          </pre>
        </div>
      )}
    </div>
  );
}
