import { useRef, useEffect } from 'react';
import { MessageCard } from './MessageCard';
import { StatusBar } from './StatusBar';
import { InputArea } from './InputArea';
import { AskUserCard } from './AskUserCard';
import type { Message, AgentStatus } from '../types';
import { Bot } from 'lucide-react';

interface ChatAreaProps {
  messages: Message[];
  agentStatus: AgentStatus;
  isWaitingForUser: boolean;
  askPrompt: string;
  onSubmitTask: (task: string) => void;
  onSendReply: (reply: string) => void;
  onStopTask: () => void;
  view: 'chat' | 'agents' | 'skills';
}

function EmptyState({ onSuggestion }: { onSuggestion: (text: string) => void }) {
  const suggestions = [
    'Read and summarize the README.md file',
    'List all files in the workspace',
    'Run a Python script to calculate fibonacci numbers',
    'Check the current git status',
  ];

  return (
    <div className="flex-1 flex flex-col items-center justify-center px-4">
      <div className="w-16 h-16 rounded-2xl bg-accent/10 flex items-center justify-center mb-6">
        <Bot className="w-8 h-8 text-accent" />
      </div>
      <h1 className="text-2xl font-semibold text-text-primary mb-2">XAgent</h1>
      <p className="text-text-secondary text-sm mb-8 text-center max-w-md">
        Your physical-level execution agent. Capable of file operations, code execution,
        browser control, and more.
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 w-full max-w-2xl">
        {suggestions.map((suggestion, index) => (
          <button
            key={index}
            onClick={() => onSuggestion(suggestion)}
            className="text-left px-4 py-3 rounded-card bg-bg-secondary border border-border hover:border-accent/30 hover:bg-bg-tertiary transition-all group"
          >
            <p className="text-sm text-text-secondary group-hover:text-text-primary transition-colors">
              {suggestion}
            </p>
          </button>
        ))}
      </div>
    </div>
  );
}

export function ChatArea({
  messages,
  agentStatus,
  isWaitingForUser,
  askPrompt,
  onSubmitTask,
  onSendReply,
  onStopTask,
  view,
}: ChatAreaProps) {
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = (text: string) => {
    if (isWaitingForUser) {
      onSendReply(text);
    } else {
      onSubmitTask(text);
    }
  };

  const isRunning = agentStatus.state !== 'idle' && agentStatus.state !== 'error';

  const showChat = view === 'chat';

  return (
    <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
      {/* Status Bar */}
      <StatusBar status={agentStatus} />

      {/* Messages Area */}
      {showChat && messages.length === 0 && (
        <EmptyState onSuggestion={handleSubmit} />
      )}

      {showChat && messages.length > 0 && (
        <div
          ref={scrollContainerRef}
          className="flex-1 overflow-y-auto px-4 py-6"
        >
          <div className="max-w-4xl mx-auto">
            {messages.map(message => (
              <MessageCard key={message.id} message={message} />
            ))}
            <div ref={messagesEndRef} />
          </div>
        </div>
      )}

      {/* Ask User Card */}
      {showChat && isWaitingForUser && askPrompt && (
        <AskUserCard prompt={askPrompt} onSubmit={onSendReply} />
      )}

      {/* Input Area - only show in chat view */}
      {showChat && (
        <InputArea
          onSubmit={handleSubmit}
          onStop={onStopTask}
          isRunning={isRunning}
          isWaitingForUser={isWaitingForUser}
          placeholder={isWaitingForUser ? 'Reply to the agent...' : 'Tell XAgent what to do...'}
        />
      )}
    </div>
  );
}
