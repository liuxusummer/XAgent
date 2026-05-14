import { useMemo, useState } from 'react';
import { Check, CheckCircle2, HelpCircle, Send } from 'lucide-react';

interface AskUserCardProps {
  prompt: string;
  onSubmit: (reply: string) => void;
}

interface AskPromptViewModel {
  title: string;
  description: string;
  options: string[];
}

function uniqueOptions(options: string[]): string[] {
  return Array.from(new Set(options.map(option => option.trim()).filter(Boolean)));
}

function parseOptions(prompt: string): string[] {
  const replyMatch = prompt.match(/(?:请回复|回复)\s*[:：]?\s*([^。.!！？\n]+)/);
  const source = replyMatch?.[1];

  if (!source) return [];

  const options = source
    .split(/\s*(?:\/|／|,|，|、|\bor\b|\|)\s*/i)
    .map(option => option.replace(/[。.!！？]+$/, '').trim());

  return uniqueOptions(options).slice(0, 6);
}

function parsePrompt(prompt: string): AskPromptViewModel {
  const cleanPrompt = prompt.replace(/^\[Agent asks\]\s*/i, '').trim();
  const withoutOptions = cleanPrompt
    .replace(/\n选项：\n(?:\d+\.\s*.+\n?)+/g, '\n')
    .replace(/\s*(?:如|若|如果)?[^。.!！？\n]{0,32}(?:请回复|回复)\s*[:：]?\s*[^。.!！？\n]+[。.!！？]?$/, '')
    .trim();
  const titleMatch = withoutOptions.match(/^(.{4,40}?[：:？?])/);
  const title = (titleMatch?.[1] || 'Agent 需要你的确认').replace(/[：:？?]$/, '').trim();
  const description = titleMatch
    ? withoutOptions.slice(titleMatch[1].length).trim()
    : withoutOptions || cleanPrompt;

  return {
    title,
    description,
    options: parseOptions(cleanPrompt),
  };
}

export function AskUserCard({ prompt, onSubmit }: AskUserCardProps) {
  const { title, description, options } = useMemo(() => parsePrompt(prompt), [prompt]);
  const [selected, setSelected] = useState('');
  const hasOptions = options.length > 0;

  const handleSubmit = () => {
    if (!selected) return;
    onSubmit(selected);
  };

  return (
    <section
      aria-labelledby="ask-user-title"
      className="mx-4 mb-3 animate-slide-up"
    >
      <div className="max-w-4xl mx-auto rounded-card border border-accent/20 bg-bg-secondary shadow-sm overflow-hidden">
        <div className="p-4 sm:p-5">
          <div className="flex items-start gap-3">
            <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-accent/10 text-accent flex items-center justify-center">
              <HelpCircle className="w-5 h-5" aria-hidden="true" />
            </div>

            <div className="min-w-0 flex-1">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-accent">
                    Agent asks
                  </p>
                  <h2 id="ask-user-title" className="mt-1 text-base font-semibold text-text-primary">
                    {title}
                  </h2>
                </div>
                {selected && (
                  <div className="inline-flex items-center gap-1.5 rounded-full bg-status-success/10 px-3 py-1 text-xs font-medium text-status-success">
                    <CheckCircle2 className="w-3.5 h-3.5" aria-hidden="true" />
                    已选择：{selected}
                  </div>
                )}
              </div>

              {description && (
                <p className="mt-3 text-sm leading-relaxed text-text-secondary whitespace-pre-wrap break-words">
                  {description}
                </p>
              )}

              {hasOptions && (
                <div
                  className="mt-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2"
                  role="group"
                  aria-label="可选回复"
                >
                  {options.map(option => {
                    const isSelected = selected === option;

                    return (
                      <button
                        key={option}
                        type="button"
                        aria-pressed={isSelected}
                        onClick={() => setSelected(option)}
                        className={`group flex items-center justify-between gap-3 rounded-button border px-3 py-2.5 text-left text-sm font-medium transition-all duration-200 active:scale-[0.98] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/60 ${
                          isSelected
                            ? 'border-accent bg-accent/15 text-text-primary shadow-sm'
                            : 'border-border bg-bg-tertiary text-text-secondary hover:border-accent/40 hover:bg-bg-hover hover:text-text-primary'
                        }`}
                      >
                        <span className="break-words">{option}</span>
                        <span
                          className={`flex h-5 w-5 flex-shrink-0 items-center justify-center rounded-full border transition-all ${
                            isSelected
                              ? 'border-accent bg-accent text-white'
                              : 'border-border group-hover:border-accent/50'
                          }`}
                        >
                          {isSelected && <Check className="w-3.5 h-3.5" aria-hidden="true" />}
                        </span>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 border-t border-border bg-bg-tertiary/60 px-4 py-3 sm:px-5">
          <p className="text-xs text-text-muted">
            {hasOptions ? '选择一个回复后点击确认，也可以在下方输入框中手动回复。' : '请在下方输入框中回复。'}
          </p>
          {hasOptions && (
            <button
              type="button"
              disabled={!selected}
              onClick={handleSubmit}
              className="inline-flex items-center justify-center gap-2 rounded-button bg-accent px-4 py-2 text-sm font-medium text-white transition-all duration-200 hover:bg-accent-hover active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
            >
              <Send className="w-4 h-4" aria-hidden="true" />
              确认提交
            </button>
          )}
        </div>
      </div>
    </section>
  );
}
