import { useState, useRef, useCallback, useEffect } from 'react';
import { Send, Square, CornerDownLeft } from 'lucide-react';

interface InputAreaProps {
  onSubmit: (message: string) => void;
  onStop: () => void;
  isRunning: boolean;
  isWaitingForUser: boolean;
  placeholder?: string;
}

export function InputArea({
  onSubmit,
  onStop,
  isRunning,
  isWaitingForUser,
  placeholder = 'Tell XAgent what to do...',
}: InputAreaProps) {
  const [input, setInput] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const adjustHeight = useCallback(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = 'auto';
    textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
  }, []);

  useEffect(() => {
    adjustHeight();
  }, [input, adjustHeight]);

  const handleSubmit = useCallback(() => {
    const trimmed = input.trim();
    if (!trimmed) return;

    onSubmit(trimmed);
    setInput('');

    // Reset height
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = 'auto';
    }
  }, [input, onSubmit]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSubmit();
      }
    },
    [handleSubmit]
  );

  const isDisabled = isRunning && !isWaitingForUser;
  const showStopButton = isRunning && !isWaitingForUser;

  return (
    <div className="border-t border-border bg-bg-secondary p-4">
      <div className="max-w-4xl mx-auto">
        <div className="relative flex items-end gap-2 bg-bg-tertiary rounded-input border border-border focus-within:border-accent/50 transition-colors">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => {
              setInput(e.target.value);
              adjustHeight();
            }}
            onKeyDown={handleKeyDown}
            placeholder={isWaitingForUser ? 'Reply to the agent...' : placeholder}
            disabled={isDisabled}
            rows={1}
            className="flex-1 bg-transparent text-sm text-text-primary placeholder-text-muted resize-none px-4 py-3 outline-none min-h-[44px] max-h-[200px] disabled:opacity-50"
          />

          <div className="flex items-center gap-1 pr-2 pb-2">
            {showStopButton ? (
              <button
                onClick={onStop}
                className="flex items-center gap-1.5 px-3 py-2 rounded-button bg-status-error/15 hover:bg-status-error/25 text-status-error transition-colors"
                title="Stop execution"
              >
                <Square className="w-3.5 h-3.5 fill-current" />
                <span className="text-xs font-medium">Stop</span>
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                disabled={!input.trim() || isDisabled}
                className="flex items-center gap-1.5 px-3 py-2 rounded-button bg-accent hover:bg-accent-hover text-white transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                title="Send message (Enter)"
              >
                <Send className="w-3.5 h-3.5" />
                <CornerDownLeft className="w-3 h-3 opacity-60" />
              </button>
            )}
          </div>
        </div>

        <div className="flex items-center justify-between mt-2 px-1">
          <div className="text-xs text-text-muted">
            {isWaitingForUser ? (
              <span className="text-status-warning">Agent is waiting for your reply</span>
            ) : isRunning ? (
              <span className="text-accent">Agent is working...</span>
            ) : (
              <span>Press Enter to send, Shift+Enter for new line</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
