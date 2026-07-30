import { useCallback, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  Clock,
  Database,
  Download,
  FileText,
  Loader2,
  Play,
  RefreshCcw,
  StopCircle,
  Upload,
  XCircle,
  FolderOpen,
  Link,
  Settings,
  List,
  ChevronLeft,
  ArrowRight,
  Tag,
  Wrench,
  MessageSquare,
} from 'lucide-react';
import { api } from '../api/client';
import type { EvalCaseResult, EvalDataset, EvalRunResult, WorkspaceAgent } from '../types';

interface EvalPanelProps {
  workspace: string;
  configPath?: string;
  observabilityConfigPath?: string;
}

type EvalTab = 'datasets' | 'run' | 'results';

function formatBytes(bytes: number | undefined): string {
  const value = bytes || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTime(seconds: number | null | undefined): string {
  if (!seconds) return '--';
  return new Date(seconds * 1000).toLocaleString();
}

function formatDuration(seconds: number | undefined): string {
  const value = seconds || 0;
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  return `${value.toFixed(1)} s`;
}

function runStatusLabel(status: string): string {
  switch (status) {
    case 'completed':
      return '完成';
    case 'running':
      return '运行中';
    case 'pending':
      return '等待';
    case 'canceling':
      return '取消中';
    case 'canceled':
      return '已取消';
    case 'error':
      return '错误';
    default:
      return status || '--';
  }
}

function caseStatusIcon(status: string, size: number = 3.5) {
  const cls = `w-${size} h-${size}`;
  if (status === 'passed') return <CheckCircle2 className={`${cls} text-status-success`} />;
  if (status === 'failed') return <XCircle className={`${cls} text-status-error`} />;
  if (status === 'error') return <AlertTriangle className={`${cls} text-status-warning`} />;
  return <Clock className={`${cls} text-text-muted`} />;
}

function statusClass(status: string): string {
  if (status === 'completed' || status === 'passed') return 'text-status-success bg-status-success/10';
  if (status === 'failed' || status === 'error') return 'text-status-error bg-status-error/10';
  if (status === 'running' || status === 'pending' || status === 'canceling') return 'text-accent bg-accent/10';
  return 'text-text-muted bg-bg-tertiary';
}

function workspaceDatasetPath(dataset: EvalDataset | null): string {
  const sourcePath = dataset?.source?.path;
  return typeof sourcePath === 'string' ? sourcePath : '';
}

function isWorkspaceDataset(dataset: EvalDataset | null): boolean {
  return dataset?.source?.type === 'workspace_path';
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-button border border-border bg-bg-secondary px-3 py-2">
      <div className="text-[11px] text-text-muted">{label}</div>
      <div className="mt-0.5 text-lg font-semibold text-text-primary">{value}</div>
    </div>
  );
}

function CaseSummaryCard({
  item,
  index,
  onClick,
}: {
  item: EvalCaseResult;
  index: number;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="text-left w-full rounded-card border border-border bg-bg-secondary p-4 transition-all hover:border-accent/30 hover:shadow-sm group"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-[11px] text-text-muted font-mono shrink-0">#{index + 1}</span>
          {caseStatusIcon(item.status)}
          <span className="truncate text-sm font-medium text-text-primary">{item.name || item.id}</span>
        </div>
        <ArrowRight className="w-4 h-4 text-text-muted shrink-0 group-hover:text-accent transition-colors" />
      </div>

      <div className="mt-2 line-clamp-2 text-xs text-text-secondary leading-relaxed">
        {item.task}
      </div>

      <div className="mt-3 flex items-center gap-3 text-[11px] text-text-muted">
        <span className="flex items-center gap-1">
          <Clock className="w-3 h-3" />
          {formatDuration(item.duration_sec)}
        </span>
        <span className="flex items-center gap-1">
          <MessageSquare className="w-3 h-3" />
          {item.turns} turns
        </span>
        {item.failures.length > 0 && (
          <span className="text-status-error">{item.failures.length} failures</span>
        )}
      </div>

      {item.failures.length > 0 && (
        <div className="mt-2 space-y-1">
          {item.failures.slice(0, 2).map((failure) => (
            <div key={failure} className="line-clamp-1 text-[11px] text-status-error">
              {failure}
            </div>
          ))}
          {item.failures.length > 2 && (
            <div className="text-[11px] text-text-muted">+{item.failures.length - 2} more</div>
          )}
        </div>
      )}
    </button>
  );
}

function CaseDetailDrawer({
  item,
  index,
  onBack,
}: {
  item: EvalCaseResult;
  index: number;
  onBack: () => void;
}) {
  return (
    <div className="h-full flex flex-col overflow-hidden bg-bg-primary">
      {/* Detail Header */}
      <div className="shrink-0 border-b border-border px-6 py-4">
        <button
          onClick={onBack}
          className="flex items-center gap-1.5 text-xs text-text-muted hover:text-text-secondary transition-colors mb-3"
        >
          <ChevronLeft className="w-4 h-4" />
          <span>Back to results</span>
        </button>

        <div className="flex items-center gap-3">
          <span className="text-[11px] text-text-muted font-mono">#{index + 1}</span>
          {caseStatusIcon(item.status, 4)}
          <h3 className="text-base font-semibold text-text-primary">{item.name || item.id}</h3>
          <span className={`ml-auto rounded-button px-2 py-1 text-xs font-medium ${statusClass(item.status)}`}>
            {item.status}
          </span>
        </div>

        <div className="mt-2 flex items-center gap-4 text-xs text-text-muted">
          <span className="flex items-center gap-1">
            <Clock className="w-3.5 h-3.5" />
            {formatDuration(item.duration_sec)}
          </span>
          <span className="flex items-center gap-1">
            <MessageSquare className="w-3.5 h-3.5" />
            {item.turns} turns
          </span>
          <span className="flex items-center gap-1">
            <Wrench className="w-3.5 h-3.5" />
            {item.tool_calls.length} tools
          </span>
          {item.exit_reason && (
            <span>Exit: {item.exit_reason}</span>
          )}
        </div>
      </div>

      {/* Detail Content */}
      <div className="flex-1 overflow-y-auto px-6 py-6">
        <div className="max-w-3xl mx-auto space-y-6">
          {/* Task */}
          <div className="bg-bg-secondary border border-border rounded-card p-5">
            <div className="flex items-center gap-2 mb-3">
              <FileText className="w-4 h-4 text-accent" />
              <h4 className="text-sm font-semibold text-text-primary">Task</h4>
            </div>
            <p className="text-sm text-text-secondary leading-relaxed whitespace-pre-wrap">{item.task}</p>
          </div>

          {/* Tags */}
          {item.tags.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <Tag className="w-3.5 h-3.5 text-text-muted" />
              {item.tags.map((tag) => (
                <span key={tag} className="text-[11px] px-2 py-0.5 rounded-full bg-bg-tertiary text-text-muted border border-border">
                  {tag}
                </span>
              ))}
            </div>
          )}

          {/* Tool Calls */}
          {item.tool_calls.length > 0 && (
            <div className="bg-bg-secondary border border-border rounded-card p-5">
              <div className="flex items-center gap-2 mb-3">
                <Wrench className="w-4 h-4 text-accent" />
                <h4 className="text-sm font-semibold text-text-primary">Tool Calls</h4>
              </div>
              <div className="flex flex-wrap gap-2">
                {item.tool_calls.map((tool) => (
                  <span key={tool} className="text-xs px-2.5 py-1 rounded-button bg-bg-primary text-text-secondary border border-border">
                    {tool}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Failures */}
          {item.failures.length > 0 && (
            <div className="bg-bg-secondary border border-border rounded-card p-5">
              <div className="flex items-center gap-2 mb-3">
                <AlertTriangle className="w-4 h-4 text-status-error" />
                <h4 className="text-sm font-semibold text-status-error">Failures</h4>
              </div>
              <div className="space-y-2">
                {item.failures.map((failure) => (
                  <div key={failure} className="text-sm text-status-error leading-relaxed">
                    {failure}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Response */}
          {item.response_excerpt && (
            <div className="bg-bg-secondary border border-border rounded-card p-5">
              <div className="flex items-center gap-2 mb-3">
                <MessageSquare className="w-4 h-4 text-accent" />
                <h4 className="text-sm font-semibold text-text-primary">Response</h4>
              </div>
              <div className="prose prose-sm max-w-full dark:prose-invert">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {item.response_excerpt}
                </ReactMarkdown>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export function EvalPanel({ workspace, configPath = '', observabilityConfigPath = '' }: EvalPanelProps) {
  const [activeTab, setActiveTab] = useState<EvalTab>('datasets');

  const [datasets, setDatasets] = useState<EvalDataset[]>([]);
  const [runs, setRuns] = useState<EvalRunResult[]>([]);
  const [agents, setAgents] = useState<WorkspaceAgent[]>([]);
  const [selectedDatasetId, setSelectedDatasetId] = useState('');
  const [selectedRunId, setSelectedRunId] = useState('');
  const [selectedRun, setSelectedRun] = useState<EvalRunResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState('');
  const [error, setError] = useState('');

  const [importName, setImportName] = useState('dataset.jsonl');
  const [importFormat, setImportFormat] = useState('jsonl');
  const [importContent, setImportContent] = useState('');
  const [importPath, setImportPath] = useState('');
  const [downloadUrl, setDownloadUrl] = useState('');
  const [downloadName, setDownloadName] = useState('');
  const [selectedAgent, setSelectedAgent] = useState('');
  const [caseLimit, setCaseLimit] = useState(0);

  const [detailCase, setDetailCase] = useState<EvalCaseResult | null>(null);
  const [detailIndex, setDetailIndex] = useState(0);

  const selectedDataset = useMemo(
    () => datasets.find((dataset) => dataset.id === selectedDatasetId) || null,
    [datasets, selectedDatasetId]
  );

  const loadLists = useCallback(async () => {
    if (!workspace) return;
    setLoading(true);
    setError('');
    const [datasetRes, runRes, agentRes] = await Promise.all([
      api.listEvalDatasets(workspace),
      api.listEvalRuns(workspace),
      api.listAgents(workspace),
    ]);
    if (datasetRes.success && datasetRes.data) {
      setDatasets(datasetRes.data);
      setSelectedDatasetId((current) => current || datasetRes.data?.[0]?.id || '');
    } else {
      setError(datasetRes.error || 'Failed to load datasets');
    }
    if (runRes.success && runRes.data) {
      setRuns(runRes.data);
      setSelectedRunId((current) => current || runRes.data?.[0]?.id || '');
    }
    if (agentRes.success && agentRes.data) {
      setAgents(agentRes.data);
      setSelectedAgent((current) => current || agentRes.data?.[0]?.name || '');
    }
    setLoading(false);
  }, [workspace]);

  const loadRun = useCallback(async () => {
    if (!workspace || !selectedRunId) {
      setSelectedRun(null);
      return;
    }
    const res = await api.getEvalRun(workspace, selectedRunId);
    if (res.success && res.data) {
      setSelectedRun(res.data);
      setRuns((prev) => prev.map((run) => (run.id === res.data?.id ? res.data : run)));
    }
  }, [workspace, selectedRunId]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSelectedDatasetId('');
      setSelectedRunId('');
      setSelectedRun(null);
      loadLists();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [workspace, loadLists]);

  useEffect(() => {
    const timer = window.setTimeout(loadRun, 0);
    return () => window.clearTimeout(timer);
  }, [loadRun]);

  useEffect(() => {
    if (!selectedRun || !['pending', 'running', 'canceling'].includes(selectedRun.status)) return;
    const timer = window.setInterval(() => {
      loadRun();
      api.listEvalRuns(workspace).then((res) => {
        if (res.success && res.data) setRuns(res.data);
      });
    }, 1500);
    return () => window.clearInterval(timer);
  }, [selectedRun, workspace, loadRun]);

  const handleImport = async () => {
    setActionLoading('import');
    setError('');
    const res = await api.importEvalDataset({
      ws: workspace,
      name: importName,
      format: importFormat,
      content: importContent,
      path: importPath,
    });
    if (res.success && res.data) {
      setImportContent('');
      setImportPath('');
      setSelectedDatasetId(res.data.id);
      await loadLists();
    } else {
      setError(res.error || 'Import failed');
    }
    setActionLoading('');
  };

  const handleDownload = async () => {
    if (!downloadUrl.trim()) return;
    setActionLoading('download');
    setError('');
    const res = await api.downloadEvalDataset({
      ws: workspace,
      url: downloadUrl,
      name: downloadName,
      format: importFormat,
    });
    if (res.success && res.data) {
      setDownloadUrl('');
      setDownloadName('');
      setSelectedDatasetId(res.data.id);
      await loadLists();
    } else {
      setError(res.error || 'Download failed');
    }
    setActionLoading('');
  };

  const handleRun = async () => {
    if (!selectedDatasetId) return;
    setActionLoading('run');
    setError('');
    let datasetId = selectedDatasetId;
    if (isWorkspaceDataset(selectedDataset)) {
      const path = workspaceDatasetPath(selectedDataset);
      const importRes = await api.importEvalDataset({
        ws: workspace,
        name: selectedDataset?.name || 'dataset',
        format: selectedDataset?.format || '',
        path,
      });
      if (importRes.success && importRes.data) {
        datasetId = importRes.data.id;
        setSelectedDatasetId(datasetId);
      } else {
        setError(importRes.error || 'Import failed');
        setActionLoading('');
        return;
      }
    }
    const res = await api.createEvalRun({
      ws: workspace,
      dataset_id: datasetId,
      agent: selectedAgent,
      case_limit: caseLimit,
      config_path: configPath,
      observability_config_path: observabilityConfigPath,
    });
    if (res.success && res.data) {
      setSelectedRunId(res.data.id);
      setSelectedRun(res.data);
      setActiveTab('results');
      await loadLists();
    } else {
      setError(res.error || 'Run failed');
    }
    setActionLoading('');
  };

  const handleCancel = async () => {
    if (!selectedRunId) return;
    setActionLoading('cancel');
    const res = await api.cancelEvalRun(workspace, selectedRunId);
    if (res.success && res.data) {
      setSelectedRun(res.data);
      await loadLists();
    } else {
      setError(res.error || 'Cancel failed');
    }
    setActionLoading('');
  };

  const summary = selectedRun?.summary;
  const canCancel = selectedRun && ['pending', 'running', 'canceling'].includes(selectedRun.status);

  const tabs: { key: EvalTab; label: string; icon: React.ReactNode }[] = [
    { key: 'datasets', label: 'Datasets', icon: <FolderOpen className="w-4 h-4" /> },
    { key: 'run', label: 'Run', icon: <Play className="w-4 h-4" /> },
    { key: 'results', label: 'Results', icon: <BarChart3 className="w-4 h-4" /> },
  ];

  return (
    <div className="flex h-full flex-1 flex-col overflow-hidden bg-bg-primary">
      {/* Header */}
      <div className="flex shrink-0 items-center justify-between px-6 pb-3 pt-5">
        <div className="flex items-center gap-3">
          <BarChart3 className="h-5 w-5 text-accent" />
          <h2 className="text-lg font-bold text-text-primary">Eval</h2>
        </div>
        <button
          onClick={loadLists}
          disabled={loading}
          className="flex items-center gap-1.5 rounded-button border border-border px-3 py-1.5 text-xs text-text-secondary transition-colors hover:bg-bg-tertiary disabled:opacity-40"
        >
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCcw className="h-3.5 w-3.5" />}
          <span>刷新</span>
        </button>
      </div>

      {error && (
        <div className="mx-6 mb-3 shrink-0 rounded-button border border-status-error/30 bg-status-error/10 px-3 py-2 text-xs text-status-error">
          {error}
        </div>
      )}

      {/* Tabs */}
      <div className="flex border-b border-border px-6 shrink-0">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium transition-colors border-b-2 ${
              activeTab === tab.key
                ? 'text-accent border-accent'
                : 'text-text-muted border-transparent hover:text-text-secondary'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div className="flex-1 overflow-hidden">
        {/* Datasets Tab */}
        {activeTab === 'datasets' && (
          <div className="h-full overflow-y-auto px-6 py-6">
            <div className="max-w-4xl mx-auto space-y-6">
              {/* Dataset List */}
              <div className="bg-bg-secondary border border-border rounded-card p-5">
                <div className="flex items-center gap-2 mb-4">
                  <div className="w-8 h-8 rounded-lg bg-accent/10 flex items-center justify-center">
                    <List className="w-4 h-4 text-accent" />
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-text-primary">Available Datasets</h3>
                    <p className="text-xs text-text-muted">{datasets.length} datasets in workspace</p>
                  </div>
                </div>

                {datasets.length > 0 ? (
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                    {datasets.map((dataset) => (
                      <button
                        key={dataset.id}
                        onClick={() => {
                          setSelectedDatasetId(dataset.id);
                          setActiveTab('run');
                        }}
                        className={`text-left rounded-card border p-4 transition-all hover:shadow-sm ${
                          selectedDatasetId === dataset.id
                            ? 'border-accent/40 bg-accent/5'
                            : 'border-border bg-bg-primary hover:border-border-hover'
                        }`}
                      >
                        <div className="flex items-center justify-between gap-2 mb-2">
                          <span className="truncate text-sm font-medium text-text-primary">{dataset.name}</span>
                          <div className="flex shrink-0 items-center gap-1">
                            <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                              isWorkspaceDataset(dataset)
                                ? 'bg-accent/10 text-accent'
                                : 'bg-status-success/10 text-status-success'
                            }`}>
                              {isWorkspaceDataset(dataset) ? 'workspace' : 'imported'}
                            </span>
                            <span className="text-[11px] text-text-muted bg-bg-tertiary px-2 py-0.5 rounded-full">
                              {dataset.case_count} cases
                            </span>
                          </div>
                        </div>
                        <div className="flex items-center gap-3 text-[11px] text-text-muted">
                          <span className="uppercase font-medium">{dataset.format}</span>
                          <span>·</span>
                          <span>{formatBytes(dataset.size_bytes)}</span>
                        </div>
                        {isWorkspaceDataset(dataset) && (
                          <div className="mt-2 truncate text-[11px] text-text-muted">
                            {workspaceDatasetPath(dataset)}
                          </div>
                        )}
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="flex flex-col items-center justify-center py-12 border border-dashed border-border rounded-card bg-bg-primary">
                    <Database className="w-8 h-8 text-text-muted opacity-40 mb-2" />
                    <p className="text-sm text-text-muted">No datasets yet</p>
                    <p className="text-xs text-text-muted mt-1">Import or download a dataset to get started</p>
                  </div>
                )}
              </div>

              {/* Import Card */}
              <div className="bg-bg-secondary border border-border rounded-card p-5">
                <div className="flex items-center gap-2 mb-4">
                  <div className="w-8 h-8 rounded-lg bg-accent/10 flex items-center justify-center">
                    <Upload className="w-4 h-4 text-accent" />
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-text-primary">Import Dataset</h3>
                    <p className="text-xs text-text-muted">Paste content or provide a file path</p>
                  </div>
                </div>

                <div className="grid grid-cols-[1fr_120px] gap-3 mb-3">
                  <input
                    value={importName}
                    onChange={(event) => setImportName(event.target.value)}
                    className="rounded-button border border-border bg-bg-primary px-3 py-2 text-xs text-text-primary outline-none"
                    placeholder="dataset.jsonl"
                  />
                  <select
                    value={importFormat}
                    onChange={(event) => setImportFormat(event.target.value)}
                    className="rounded-button border border-border bg-bg-primary px-3 py-2 text-xs text-text-primary outline-none"
                  >
                    <option value="jsonl">JSONL</option>
                    <option value="json">JSON</option>
                    <option value="csv">CSV</option>
                  </select>
                </div>

                <input
                  value={importPath}
                  onChange={(event) => setImportPath(event.target.value)}
                  className="w-full rounded-button border border-border bg-bg-primary px-3 py-2 text-xs text-text-primary outline-none mb-3"
                  placeholder="business/eval.jsonl (optional file path)"
                />

                <textarea
                  value={importContent}
                  onChange={(event) => setImportContent(event.target.value)}
                  className="h-40 w-full resize-none rounded-button border border-border bg-bg-primary px-3 py-2 font-mono text-xs leading-relaxed text-text-primary outline-none"
                  spellCheck={false}
                  placeholder='{"id":"case-001","task":"...","assertions":{"contains":["..."]}}'
                />

                <button
                  onClick={handleImport}
                  disabled={actionLoading === 'import' || (!importContent.trim() && !importPath.trim())}
                  className="mt-3 flex items-center justify-center gap-1.5 rounded-button bg-accent px-4 py-2 text-xs font-medium text-white transition-colors hover:bg-accent-hover disabled:opacity-40"
                >
                  {actionLoading === 'import' ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
                  <span>Import</span>
                </button>
              </div>

              {/* Download Card */}
              <div className="bg-bg-secondary border border-border rounded-card p-5">
                <div className="flex items-center gap-2 mb-4">
                  <div className="w-8 h-8 rounded-lg bg-accent/10 flex items-center justify-center">
                    <Link className="w-4 h-4 text-accent" />
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-text-primary">Download from URL</h3>
                    <p className="text-xs text-text-muted">Fetch a dataset from a remote URL</p>
                  </div>
                </div>

                <div className="grid grid-cols-[1fr_1fr] gap-3 mb-3">
                  <input
                    value={downloadUrl}
                    onChange={(event) => setDownloadUrl(event.target.value)}
                    className="rounded-button border border-border bg-bg-primary px-3 py-2 text-xs text-text-primary outline-none"
                    placeholder="https://example.com/eval.jsonl"
                  />
                  <input
                    value={downloadName}
                    onChange={(event) => setDownloadName(event.target.value)}
                    className="rounded-button border border-border bg-bg-primary px-3 py-2 text-xs text-text-primary outline-none"
                    placeholder="downloaded-suite.jsonl"
                  />
                </div>

                <button
                  onClick={handleDownload}
                  disabled={actionLoading === 'download' || !downloadUrl.trim()}
                  className="flex items-center justify-center gap-1.5 rounded-button border border-border px-4 py-2 text-xs font-medium text-text-secondary transition-colors hover:bg-bg-tertiary disabled:opacity-40"
                >
                  {actionLoading === 'download' ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Download className="h-3.5 w-3.5" />}
                  <span>Download</span>
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Run Tab */}
        {activeTab === 'run' && (
          <div className="h-full overflow-y-auto px-6 py-6">
            <div className="max-w-2xl mx-auto">
              <div className="bg-bg-secondary border border-border rounded-card p-6 space-y-5">
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-8 h-8 rounded-lg bg-accent/10 flex items-center justify-center">
                    <Settings className="w-4 h-4 text-accent" />
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-text-primary">Run Configuration</h3>
                    <p className="text-xs text-text-muted">Select dataset and agent to evaluate</p>
                  </div>
                </div>

                <div className="space-y-4">
                  <div>
                    <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                      Dataset
                    </label>
                    <select
                      value={selectedDatasetId}
                      onChange={(event) => setSelectedDatasetId(event.target.value)}
                      className="w-full rounded-button border border-border bg-bg-primary px-3 py-2.5 text-xs text-text-primary outline-none"
                    >
                      <option value="">Select a dataset</option>
                      {datasets.map((dataset) => (
                        <option key={dataset.id} value={dataset.id}>
                          {dataset.name} ({dataset.case_count} cases, {isWorkspaceDataset(dataset) ? 'workspace' : 'imported'})
                        </option>
                      ))}
                    </select>
                    {selectedDataset && (
                      <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-text-muted">
                        <span>{selectedDataset.format.toUpperCase()}</span>
                        <span>·</span>
                        <span>{formatBytes(selectedDataset.size_bytes)}</span>
                        <span>·</span>
                        <span>{formatTime(selectedDataset.created_at)}</span>
                        {isWorkspaceDataset(selectedDataset) && (
                          <>
                            <span>·</span>
                            <span className="truncate">{workspaceDatasetPath(selectedDataset)}</span>
                          </>
                        )}
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-[1fr_120px] gap-3">
                    <div>
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        Agent
                      </label>
                      <select
                        value={selectedAgent}
                        onChange={(event) => setSelectedAgent(event.target.value)}
                        className="w-full rounded-button border border-border bg-bg-primary px-3 py-2.5 text-xs text-text-primary outline-none"
                      >
                        <option value="">default</option>
                        {agents.map((agent) => (
                          <option key={agent.name} value={agent.name}>
                            {agent.name}
                          </option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        Case Limit
                      </label>
                      <input
                        type="number"
                        min={0}
                        max={500}
                        value={caseLimit}
                        onChange={(event) => setCaseLimit(Number(event.target.value))}
                        className="w-full rounded-button border border-border bg-bg-primary px-3 py-2.5 text-xs text-text-primary outline-none"
                        placeholder="0 = all"
                      />
                    </div>
                  </div>
                </div>

                <div className="flex gap-3 pt-2">
                  <button
                    onClick={handleRun}
                    disabled={actionLoading === 'run' || !selectedDatasetId}
                    className="flex flex-1 items-center justify-center gap-1.5 rounded-button bg-accent px-4 py-2.5 text-xs font-medium text-white transition-colors hover:bg-accent-hover disabled:opacity-40"
                  >
                    {actionLoading === 'run' ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                    <span>Run Evaluation</span>
                  </button>
                  <button
                    onClick={handleCancel}
                    disabled={!canCancel || actionLoading === 'cancel'}
                    className="flex items-center justify-center gap-1.5 rounded-button border border-border px-4 py-2.5 text-xs font-medium text-text-secondary transition-colors hover:bg-bg-tertiary disabled:opacity-40"
                  >
                    {actionLoading === 'cancel' ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <StopCircle className="h-3.5 w-3.5" />}
                    <span>Cancel</span>
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Results Tab */}
        {activeTab === 'results' && (
          <div className="h-full overflow-hidden">
            {detailCase ? (
              <CaseDetailDrawer
                item={detailCase}
                index={detailIndex}
                onBack={() => setDetailCase(null)}
              />
            ) : (
              <div className="h-full flex flex-col overflow-hidden">
                {/* Run selector + dataset info */}
                <div className="grid shrink-0 grid-cols-[1fr_320px] border-b border-border">
                  <div className="min-w-0 px-4 py-3">
                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-accent" />
                      <span className="truncate text-sm font-semibold text-text-primary">
                        {selectedDataset ? selectedDataset.name : 'No dataset selected'}
                      </span>
                    </div>
                    <div className="mt-1 text-xs text-text-muted">
                      {selectedDataset
                        ? `${selectedDataset.case_count} cases · ${formatBytes(selectedDataset.size_bytes)} · ${formatTime(selectedDataset.created_at)}`
                        : '--'}
                    </div>
                  </div>
                  <div className="border-l border-border px-4 py-3">
                    <select
                      value={selectedRunId}
                      onChange={(event) => setSelectedRunId(event.target.value)}
                      className="w-full rounded-button border border-border bg-bg-secondary px-3 py-2 text-xs text-text-primary outline-none"
                    >
                      <option value="">选择运行</option>
                      {runs.map((run) => (
                        <option key={run.id} value={run.id}>
                          {runStatusLabel(run.status)} · {run.dataset_name}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                {selectedRun ? (
                  <>
                    {/* Metrics */}
                    <div className="shrink-0 border-b border-border p-4">
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <span className={`rounded-button px-2 py-1 text-xs font-medium ${statusClass(selectedRun.status)}`}>
                              {runStatusLabel(selectedRun.status)}
                            </span>
                            <span className="truncate text-sm font-semibold text-text-primary">{selectedRun.id}</span>
                          </div>
                          <div className="mt-1 text-xs text-text-muted">
                            agent {selectedRun.agent || 'default'} · started {formatTime(selectedRun.started_at)}
                          </div>
                        </div>
                      </div>
                      <div className="grid grid-cols-7 gap-3">
                        <Metric label="Total" value={summary?.total ?? 0} />
                        <Metric label="Done" value={summary?.completed ?? 0} />
                        <Metric label="Passed" value={summary?.passed ?? 0} />
                        <Metric label="Failed" value={summary?.failed ?? 0} />
                        <Metric label="Errors" value={summary?.error ?? 0} />
                        <Metric label="Pass Rate" value={`${Math.round((summary?.pass_rate ?? 0) * 100)}%`} />
                        <Metric label="Avg Turns" value={(summary?.avg_turns ?? 0).toFixed(1)} />
                      </div>
                      {selectedRun.version >= 2 && (
                        <div className="mt-3 grid grid-cols-6 gap-3">
                          <Metric label="P95 Latency" value={formatDuration(summary?.p95_duration ?? 0)} />
                          <Metric label="Tool Attempts" value={summary?.tool_attempts ?? 0} />
                          <Metric
                            label="Tool Success"
                            value={`${Math.round((summary?.tool_success_rate ?? 0) * 100)}%`}
                          />
                          <Metric
                            label="Recovery"
                            value={
                              (summary?.recovery_opportunities ?? 0) > 0
                                ? `${Math.round((summary?.recovery_rate ?? 0) * 100)}%`
                                : '--'
                            }
                          />
                          <Metric
                            label="Avg Tokens"
                            value={
                              (summary?.total_token_coverage ?? 0) > 0
                                ? Math.round(summary?.avg_total_tokens ?? 0)
                                : '--'
                            }
                          />
                          <Metric
                            label="Token Coverage"
                            value={`${Math.round((summary?.total_token_coverage ?? 0) * 100)}%`}
                          />
                        </div>
                      )}
                    </div>

                    {/* Cases grid */}
                    <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
                      <div className="max-w-4xl mx-auto">
                        {selectedRun.cases.length > 0 ? (
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                            {selectedRun.cases.map((item, idx) => (
                              <CaseSummaryCard
                                key={item.id}
                                item={item}
                                index={idx}
                                onClick={() => {
                                  setDetailCase(item);
                                  setDetailIndex(idx);
                                }}
                              />
                            ))}
                          </div>
                        ) : (
                          <div className="flex h-64 items-center justify-center text-text-muted">
                            <div className="text-center">
                              <FileText className="mx-auto mb-3 h-10 w-10 opacity-20" />
                              <p className="text-sm">暂无 case 结果</p>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="flex flex-1 items-center justify-center text-text-muted">
                    <div className="text-center">
                      <BarChart3 className="mx-auto mb-3 h-10 w-10 opacity-20" />
                      <p className="text-sm">暂无运行结果</p>
                    </div>
                    </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
