import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertCircle,
  Clock3,
  Database,
  FileJson,
  Gauge,
  Loader2,
  Radio,
  RefreshCw,
  Timer,
  Zap,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { api } from '../api/client';
import type { LiveTokenUsage, TokenUsage, TraceEvent, TraceSessionDetail, UsageSession, UsageSummary } from '../types';

interface UsagePanelProps {
  workspace: string;
  observabilityConfigPath?: string;
  liveUsage: LiveTokenUsage | null;
}

const EMPTY_USAGE: TokenUsage = {
  input_tokens: 0,
  output_tokens: 0,
  total_tokens: 0,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 0,
  reasoning_tokens: 0,
};

function formatNumber(value: number | undefined): string {
  return (value || 0).toLocaleString();
}

function formatTime(seconds: number | undefined): string {
  if (!seconds) return 'Never';
  return new Date(seconds * 1000).toLocaleString();
}

function formatDuration(ms: number | undefined): string {
  const seconds = Math.max(0, Math.round((ms || 0) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${seconds % 60}s`;
}

function mergeUsage(usage?: Partial<TokenUsage>): TokenUsage {
  return { ...EMPTY_USAGE, ...(usage || {}) };
}

function usageHasDetails(usage: TokenUsage): boolean {
  return (
    usage.reasoning_tokens > 0 ||
    usage.cache_creation_input_tokens > 0 ||
    usage.cache_read_input_tokens > 0
  );
}

function formatEventData(event: TraceEvent): string {
  const data = event.data || {};
  const parts: string[] = [];
  if (typeof event.duration_ms === 'number') {
    parts.push(formatDuration(event.duration_ms));
  }
  for (const key of ['tool_count', 'tool_call_count', 'content_len', 'status', 'should_exit']) {
    const value = data[key];
    if (value !== undefined) parts.push(`${key}=${String(value)}`);
  }
  const usage = mergeUsage(data as Partial<TokenUsage>);
  if (usage.total_tokens > 0) parts.push(`tokens=${formatNumber(usage.total_tokens)}`);
  return parts.join(' · ');
}

function MetricTile({
  label,
  value,
  icon: Icon,
  tone,
}: {
  label: string;
  value: string;
  icon: LucideIcon;
  tone: string;
}) {
  return (
    <div className="rounded-card border border-border bg-bg-secondary px-4 py-3 min-h-[96px]">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-text-muted uppercase tracking-wider">{label}</span>
        <Icon className={`w-4 h-4 ${tone}`} />
      </div>
      <div className="mt-3 text-2xl font-semibold text-text-primary tabular-nums">{value}</div>
    </div>
  );
}

function TokenSplit({ usage }: { usage: TokenUsage }) {
  const total = Math.max(usage.input_tokens + usage.output_tokens, 1);
  const inputPct = Math.round((usage.input_tokens / total) * 100);
  const outputPct = Math.max(0, 100 - inputPct);

  return (
    <div className="rounded-card border border-border bg-bg-secondary px-4 py-3">
      <div className="flex items-center justify-between gap-4">
        <div>
          <div className="text-xs font-medium text-text-muted uppercase tracking-wider">Input / Output</div>
          <div className="mt-1 text-sm text-text-secondary">
            {formatNumber(usage.input_tokens)} in · {formatNumber(usage.output_tokens)} out
          </div>
        </div>
        <div className="text-sm font-semibold text-text-primary">{inputPct}% / {outputPct}%</div>
      </div>
      <div className="mt-3 h-2 rounded-full bg-bg-tertiary overflow-hidden flex">
        <div className="bg-cyan-500" style={{ width: `${inputPct}%` }} />
        <div className="bg-emerald-500" style={{ width: `${outputPct}%` }} />
      </div>
    </div>
  );
}

function SessionSparkline({ sessions }: { sessions: UsageSession[] }) {
  const recent = sessions.slice(0, 12).reverse();
  const max = Math.max(...recent.map(item => item.usage.total_tokens), 1);

  return (
    <div className="flex items-end gap-1 h-20">
      {recent.length === 0 ? (
        <div className="w-full text-xs text-text-muted">No task history yet.</div>
      ) : (
        recent.map(item => (
          <div
            key={item.session_id}
            className="flex-1 rounded-t-sm bg-accent/70 min-w-[8px]"
            title={`${item.session_id}: ${formatNumber(item.usage.total_tokens)} tokens`}
            style={{ height: `${Math.max(8, (item.usage.total_tokens / max) * 80)}px` }}
          />
        ))
      )}
    </div>
  );
}

export function UsagePanel({ workspace, observabilityConfigPath = '', liveUsage }: UsagePanelProps) {
  const [summary, setSummary] = useState<UsageSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedSessionId, setSelectedSessionId] = useState('');
  const [traceDetail, setTraceDetail] = useState<TraceSessionDetail | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceError, setTraceError] = useState('');

  useEffect(() => {
    setSelectedSessionId('');
    setTraceDetail(null);
  }, [workspace, observabilityConfigPath]);

  const loadSummary = useCallback(() => {
    setLoading(true);
    setError('');
    api.getUsageSummary({ ws: workspace, observabilityConfigPath, limit: 30 })
      .then((res) => {
        if (res.success && res.data) {
          const usageSummary = res.data;
          setSummary(usageSummary);
          if (usageSummary.sessions.length > 0) {
            setSelectedSessionId(current => current || usageSummary.sessions[0].session_id);
          }
        } else {
          setError(res.error || 'Failed to load usage summary');
        }
      })
      .finally(() => setLoading(false));
  }, [workspace, observabilityConfigPath]);

  useEffect(() => {
    loadSummary();
  }, [loadSummary]);

  useEffect(() => {
    if (!selectedSessionId) {
      setTraceDetail(null);
      setTraceError('');
      return;
    }
    setTraceLoading(true);
    setTraceError('');
    api.getTraceSession(selectedSessionId, { ws: workspace, observabilityConfigPath })
      .then((res) => {
        if (res.success && res.data) {
          setTraceDetail(res.data);
        } else {
          setTraceDetail(null);
          setTraceError(res.error || 'Failed to load trace session');
        }
      })
      .finally(() => setTraceLoading(false));
  }, [selectedSessionId, workspace, observabilityConfigPath]);

  const totals = mergeUsage(summary?.totals);
  const liveTotals = mergeUsage(liveUsage?.totals);
  const sessions = summary?.sessions || [];
  const latestSession = sessions[0];

  const tableRows = useMemo(() => sessions.slice(0, 12), [sessions]);
  const timelineEvents = traceDetail?.events || [];

  return (
    <div className="flex-1 flex flex-col min-w-0 bg-bg-primary">
      <div className="px-6 py-5 border-b border-border bg-bg-primary shrink-0">
        <div className="flex items-center justify-between gap-4">
          <div>
            <div className="flex items-center gap-2">
              <Gauge className="w-5 h-5 text-accent" />
              <h1 className="text-xl font-semibold text-text-primary">Token Usage</h1>
            </div>
            <div className="mt-1 text-sm text-text-muted">
              Historical telemetry, live task consumption, and structured trace drill-down.
            </div>
          </div>
          <button
            onClick={loadSummary}
            disabled={loading}
            className="flex items-center gap-2 px-3 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-50"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            <span>Refresh</span>
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-5 space-y-5">
        {error && (
          <div className="rounded-card border border-status-error/30 bg-status-error/10 px-4 py-3 text-sm text-status-error">
            {error}
          </div>
        )}

        {summary && !summary.configured && (
          <div className="rounded-card border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-600 dark:text-amber-300 flex items-start gap-3">
            <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
            <span>{summary.message}</span>
          </div>
        )}

        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">
          <MetricTile label="Total Tokens" value={formatNumber(totals.total_tokens)} icon={Zap} tone="text-amber-500" />
          <MetricTile label="Input Tokens" value={formatNumber(totals.input_tokens)} icon={Database} tone="text-cyan-500" />
          <MetricTile label="Output Tokens" value={formatNumber(totals.output_tokens)} icon={Activity} tone="text-emerald-500" />
          <MetricTile label="Runs Tracked" value={formatNumber(sessions.length)} icon={Clock3} tone="text-rose-500" />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[1.1fr_0.9fr] gap-4">
          <div className="rounded-card border border-border bg-bg-secondary p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2 text-sm font-semibold text-text-primary">
                  <Radio className={`w-4 h-4 ${liveUsage?.running ? 'text-status-success' : 'text-text-muted'}`} />
                  <span>Live Run</span>
                </div>
                <div className="mt-1 text-xs text-text-muted">
                  {liveUsage ? `session ${liveUsage.session_id || 'current'} · turn ${liveUsage.turn || 0}` : 'No active token stream'}
                </div>
              </div>
              <div className="text-right">
                <div className="text-2xl font-semibold text-text-primary tabular-nums">
                  {formatNumber(liveTotals.total_tokens)}
                </div>
                <div className="text-xs text-text-muted">tokens</div>
              </div>
            </div>
            <div className="mt-4 grid grid-cols-3 gap-2">
              <div className="rounded-button bg-bg-tertiary px-3 py-2">
                <div className="text-[11px] text-text-muted">Input</div>
                <div className="text-sm font-semibold text-text-primary">{formatNumber(liveTotals.input_tokens)}</div>
              </div>
              <div className="rounded-button bg-bg-tertiary px-3 py-2">
                <div className="text-[11px] text-text-muted">Output</div>
                <div className="text-sm font-semibold text-text-primary">{formatNumber(liveTotals.output_tokens)}</div>
              </div>
              <div className="rounded-button bg-bg-tertiary px-3 py-2">
                <div className="text-[11px] text-text-muted">Updated</div>
                <div className="text-sm font-semibold text-text-primary">
                  {liveUsage ? formatTime(liveUsage.updated_at) : 'Never'}
                </div>
              </div>
            </div>
          </div>

          <div className="rounded-card border border-border bg-bg-secondary p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-semibold text-text-primary">Recent Shape</div>
                <div className="mt-1 text-xs text-text-muted">Last {Math.min(12, sessions.length)} completed runs</div>
              </div>
              <Timer className="w-4 h-4 text-text-muted" />
            </div>
            <div className="mt-4">
              <SessionSparkline sessions={sessions} />
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[0.85fr_1.15fr] gap-4">
          <div className="space-y-3">
            <TokenSplit usage={totals} />
            {usageHasDetails(totals) && (
              <div className="rounded-card border border-border bg-bg-secondary px-4 py-3">
                <div className="text-xs font-medium text-text-muted uppercase tracking-wider">Advanced Tokens</div>
                <div className="mt-3 grid grid-cols-1 sm:grid-cols-3 gap-2">
                  <div className="rounded-button bg-bg-tertiary px-3 py-2">
                    <div className="text-[11px] text-text-muted">Reasoning</div>
                    <div className="text-sm font-semibold text-text-primary">{formatNumber(totals.reasoning_tokens)}</div>
                  </div>
                  <div className="rounded-button bg-bg-tertiary px-3 py-2">
                    <div className="text-[11px] text-text-muted">Cache Create</div>
                    <div className="text-sm font-semibold text-text-primary">{formatNumber(totals.cache_creation_input_tokens)}</div>
                  </div>
                  <div className="rounded-button bg-bg-tertiary px-3 py-2">
                    <div className="text-[11px] text-text-muted">Cache Read</div>
                    <div className="text-sm font-semibold text-text-primary">{formatNumber(totals.cache_read_input_tokens)}</div>
                  </div>
                </div>
              </div>
            )}
            {latestSession && (
              <div className="rounded-card border border-border bg-bg-secondary px-4 py-3">
                <div className="text-xs font-medium text-text-muted uppercase tracking-wider">Latest Run</div>
                <div className="mt-2 text-sm text-text-secondary">
                  {latestSession.exit_reason || 'unknown'} · {latestSession.turns} turns · {formatDuration(latestSession.duration_ms)}
                </div>
                <div className="mt-2 font-mono text-xs text-text-muted truncate">{latestSession.session_id}</div>
              </div>
            )}
          </div>

          <div className="rounded-card border border-border bg-bg-secondary overflow-hidden">
            <div className="px-4 py-3 border-b border-border flex items-center justify-between">
              <div>
                <div className="text-sm font-semibold text-text-primary">Recent Tasks</div>
                <div className="text-xs text-text-muted mt-1">
                  {summary?.log_dir ? summary.log_dir : 'Telemetry log directory not configured'}
                </div>
              </div>
              <div className="text-xs text-text-muted">{summary ? formatTime(summary.updated_at) : ''}</div>
            </div>

            {loading && !summary ? (
              <div className="flex items-center justify-center py-12 text-text-muted">
                <Loader2 className="w-4 h-4 animate-spin mr-2" />
                <span className="text-sm">Loading usage...</span>
              </div>
            ) : tableRows.length === 0 ? (
              <div className="px-4 py-10 text-center text-text-muted">
                <Gauge className="w-8 h-8 mx-auto mb-3 opacity-30" />
                <div className="text-sm">{summary?.message || 'No token usage recorded yet.'}</div>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="text-xs text-text-muted uppercase tracking-wider bg-bg-tertiary/70">
                    <tr>
                      <th className="text-left font-medium px-4 py-2">Session</th>
                      <th className="text-right font-medium px-3 py-2">Tokens</th>
                      <th className="text-right font-medium px-3 py-2">In</th>
                      <th className="text-right font-medium px-3 py-2">Out</th>
                      <th className="text-left font-medium px-3 py-2">Exit</th>
                      <th className="text-right font-medium px-3 py-2">Events</th>
                      <th className="text-right font-medium px-4 py-2">Duration</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {tableRows.map((item) => (
                      <tr
                        key={item.session_id}
                        onClick={() => setSelectedSessionId(item.session_id)}
                        className={`cursor-pointer hover:bg-bg-tertiary/50 ${
                          selectedSessionId === item.session_id ? 'bg-bg-tertiary/70' : ''
                        }`}
                      >
                        <td className="px-4 py-2 font-mono text-xs text-text-secondary max-w-[160px] truncate">
                          {item.session_id}
                        </td>
                        <td className="px-3 py-2 text-right font-semibold text-text-primary tabular-nums">
                          {formatNumber(item.usage.total_tokens)}
                        </td>
                        <td className="px-3 py-2 text-right text-text-secondary tabular-nums">
                          {formatNumber(item.usage.input_tokens)}
                        </td>
                        <td className="px-3 py-2 text-right text-text-secondary tabular-nums">
                          {formatNumber(item.usage.output_tokens)}
                        </td>
                        <td className="px-3 py-2 text-text-secondary whitespace-nowrap">
                          {item.exit_reason || 'unknown'}
                        </td>
                        <td className="px-3 py-2 text-right text-text-secondary tabular-nums">
                          {formatNumber(item.event_count)}
                        </td>
                        <td className="px-4 py-2 text-right text-text-secondary whitespace-nowrap">
                          {formatDuration(item.duration_ms)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        <div className="rounded-card border border-border bg-bg-secondary overflow-hidden">
          <div className="px-4 py-3 border-b border-border flex items-center justify-between gap-4">
            <div className="min-w-0">
              <div className="flex items-center gap-2 text-sm font-semibold text-text-primary">
                <FileJson className="w-4 h-4 text-accent" />
                <span>Trace Timeline</span>
              </div>
              <div className="mt-1 font-mono text-xs text-text-muted truncate">
                {traceDetail?.log_path || selectedSessionId || 'Select a recent task'}
              </div>
            </div>
            {traceLoading && <Loader2 className="w-4 h-4 animate-spin text-text-muted shrink-0" />}
          </div>

          {traceError ? (
            <div className="px-4 py-8 text-sm text-status-error">{traceError}</div>
          ) : !selectedSessionId ? (
            <div className="px-4 py-10 text-center text-text-muted">
              <FileJson className="w-8 h-8 mx-auto mb-3 opacity-30" />
              <div className="text-sm">Run a task, then select it here to inspect its structured trace.</div>
            </div>
          ) : timelineEvents.length === 0 ? (
            <div className="px-4 py-8 text-sm text-text-muted">
              {traceLoading ? 'Loading trace...' : 'No events recorded for this session.'}
            </div>
          ) : (
            <div className="divide-y divide-border">
              {timelineEvents.map((event, index) => {
                const eventData = formatEventData(event);
                return (
                  <div key={`${event.kind}-${event.turn}-${event.ts}-${index}`} className="px-4 py-3 grid grid-cols-[92px_1fr] gap-3">
                    <div className="text-xs text-text-muted tabular-nums">
                      {event.turn > 0 ? `turn ${event.turn}` : 'run'}
                    </div>
                    <div className="min-w-0">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="text-sm font-semibold text-text-primary">{event.kind}</span>
                        {event.name && <span className="text-xs text-text-secondary truncate">{event.name}</span>}
                      </div>
                      <div className="mt-1 text-xs text-text-muted">
                        {formatTime(event.ts)}{eventData ? ` · ${eventData}` : ''}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
