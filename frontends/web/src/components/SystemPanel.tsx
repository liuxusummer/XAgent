import { useState, useEffect, useCallback } from 'react';
import {
  Shield,
  Loader2,
  Save,
  FileText,
  Folder,
  ChevronRight,
  ChevronDown,
  RotateCcw,
  FolderOpen,
  Search,
  Database,
  Eye,
  FileSearch,
} from 'lucide-react';
import { api } from '../api/client';
import type {
  WorkspaceIndexMatch,
  WorkspaceIndexRefreshResult,
  WorkspaceIndexSearchResult,
  WorkspaceIndexStats,
  WorkspacePreviewFile,
} from '../types';

interface TreeNode {
  name: string;
  path: string;
  type: 'file' | 'dir';
  children?: TreeNode[];
}

interface SystemPanelProps {
  workspace: string;
}

type SystemTab = 'files' | 'index';

function formatBytes(bytes: number | undefined): string {
  const value = bytes || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTime(seconds: number | null | undefined): string {
  if (!seconds) return 'Never';
  return new Date(seconds * 1000).toLocaleString();
}

function TreeItem({
  node,
  level,
  selectedPath,
  onSelect,
  expandedPaths,
  onToggleExpand,
}: {
  node: TreeNode;
  level: number;
  selectedPath: string | null;
  onSelect: (path: string, type: 'file' | 'dir') => void;
  expandedPaths: Set<string>;
  onToggleExpand: (path: string) => void;
}) {
  const isExpanded = expandedPaths.has(node.path);
  const isSelected = selectedPath === node.path;
  const hasChildren = node.type === 'dir' && node.children && node.children.length > 0;

  return (
    <div>
      <button
        onClick={() => {
          if (node.type === 'dir') {
            onToggleExpand(node.path);
          }
          onSelect(node.path, node.type);
        }}
        className={`flex items-center gap-1.5 w-full px-2 py-1.5 rounded-button text-sm transition-colors ${
          isSelected
            ? 'bg-accent/10 text-accent'
            : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
        }`}
        style={{ paddingLeft: `${level * 16 + 8}px` }}
      >
        {node.type === 'dir' && hasChildren && (
          <span className="shrink-0">
            {isExpanded ? (
              <ChevronDown className="w-3.5 h-3.5" />
            ) : (
              <ChevronRight className="w-3.5 h-3.5" />
            )}
          </span>
        )}
        {node.type === 'dir' && !hasChildren && (
          <span className="w-3.5 shrink-0" />
        )}
        {node.type === 'dir' ? (
          <FolderOpen className="w-3.5 h-3.5 shrink-0 text-accent/70" />
        ) : (
          <FileText className="w-3.5 h-3.5 shrink-0 text-text-muted" />
        )}
        <span className="truncate">{node.name}</span>
      </button>
      {node.type === 'dir' && isExpanded && node.children && (
        <div>
          {node.children.map((child) => (
            <TreeItem
              key={child.path}
              node={child}
              level={level + 1}
              selectedPath={selectedPath}
              onSelect={onSelect}
              expandedPaths={expandedPaths}
              onToggleExpand={onToggleExpand}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export function SystemPanel({ workspace }: SystemPanelProps) {
  const [activeTab, setActiveTab] = useState<SystemTab>('files');
  const [tree, setTree] = useState<TreeNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [selectedType, setSelectedType] = useState<'file' | 'dir'>('file');
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set());
  const [fileContent, setFileContent] = useState('');
  const [fileLoading, setFileLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [indexStats, setIndexStats] = useState<WorkspaceIndexStats | null>(null);
  const [indexLoading, setIndexLoading] = useState(false);
  const [indexRefreshing, setIndexRefreshing] = useState(false);
  const [indexQuery, setIndexQuery] = useState('');
  const [indexLimit, setIndexLimit] = useState(20);
  const [indexPathOnly, setIndexPathOnly] = useState(false);
  const [indexResult, setIndexResult] = useState<WorkspaceIndexSearchResult | null>(null);
  const [indexError, setIndexError] = useState('');
  const [lastRefresh, setLastRefresh] = useState<WorkspaceIndexRefreshResult | null>(null);
  const [previewFile, setPreviewFile] = useState<WorkspacePreviewFile | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState('');

  const loadTree = useCallback(() => {
    if (!workspace) return;
    setLoading(true);
    api.getWorkspaceTree(workspace)
      .then((res) => {
        if (res.success && res.data) {
          setTree(res.data);
          // Auto-expand root
          setExpandedPaths(new Set([res.data.path]));
        }
      })
      .finally(() => setLoading(false));
  }, [workspace]);

  useEffect(() => {
    loadTree();
  }, [loadTree]);

  const loadIndexStats = useCallback(() => {
    if (!workspace) return;
    setIndexLoading(true);
    api.getWorkspaceIndexStats(workspace)
      .then((res) => {
        if (res.success && res.data) {
          setIndexStats(res.data);
        } else {
          setIndexError(res.error || 'Failed to load index stats');
        }
      })
      .finally(() => setIndexLoading(false));
  }, [workspace]);

  useEffect(() => {
    if (activeTab === 'index') {
      loadIndexStats();
    }
  }, [activeTab, loadIndexStats]);

  const handleToggleExpand = (path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  const handleSelect = (path: string, type: 'file' | 'dir') => {
    setSelectedPath(path);
    setSelectedType(type);
    if (type === 'file') {
      setFileLoading(true);
      api.readWorkspaceFile(workspace, path)
        .then((res) => {
          if (res.success && res.data) {
            setFileContent(res.data.content);
          } else {
            setFileContent('');
          }
        })
        .catch(() => setFileContent(''))
        .finally(() => setFileLoading(false));
    }
  };

  const handleSave = async () => {
    if (!selectedPath || selectedType !== 'file') return;
    setSaving(true);
    setSaveStatus('idle');
    const res = await api.writeWorkspaceFile(workspace, selectedPath, fileContent);
    if (res.success) {
      setSaveStatus('success');
      setTimeout(() => setSaveStatus('idle'), 2000);
    } else {
      setSaveStatus('error');
    }
    setSaving(false);
  };

  const handleReset = () => {
    if (selectedPath && selectedType === 'file') {
      handleSelect(selectedPath, 'file');
    }
  };

  const handleRefreshIndex = async () => {
    setIndexRefreshing(true);
    setIndexError('');
    const res = await api.refreshWorkspaceIndex(workspace);
    if (res.success && res.data) {
      setLastRefresh(res.data);
      await api.getWorkspaceIndexStats(workspace).then((statsRes) => {
        if (statsRes.success && statsRes.data) setIndexStats(statsRes.data);
      });
      if (indexQuery.trim()) {
        await handleSearchIndex(false);
      }
    } else {
      setIndexError(res.error || 'Failed to refresh index');
    }
    setIndexRefreshing(false);
  };

  const handleSearchIndex = async (forceRefresh = false) => {
    const query = indexQuery.trim();
    if (!query) {
      setIndexResult(null);
      return;
    }
    setIndexLoading(true);
    setIndexError('');
    const res = await api.searchWorkspaceIndex(workspace, query, {
      limit: indexLimit,
      refresh: forceRefresh,
      pathOnly: indexPathOnly,
    });
    if (res.success && res.data) {
      setIndexResult(res.data);
      if (res.data.refresh_stats) {
        setLastRefresh(res.data.refresh_stats);
      }
      await api.getWorkspaceIndexStats(workspace).then((statsRes) => {
        if (statsRes.success && statsRes.data) setIndexStats(statsRes.data);
      });
    } else {
      setIndexResult(null);
      setIndexError(res.error || 'Search failed');
    }
    setIndexLoading(false);
  };

  const handlePreviewMatch = async (match: WorkspaceIndexMatch) => {
    setPreviewLoading(true);
    setPreviewError('');
    setPreviewFile(null);
    const res = await api.previewWorkspaceFile(workspace, match.path);
    if (res.success && res.data) {
      setPreviewFile(res.data);
    } else {
      setPreviewError(res.error || 'Preview failed');
    }
    setPreviewLoading(false);
  };

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden bg-bg-primary">
      {/* Header */}
      <div className="flex items-center justify-between px-6 pt-5 pb-3 shrink-0">
        <div className="flex items-center gap-3">
          <Shield className="w-5 h-5 text-accent" />
          <h2 className="text-lg font-bold text-text-primary">System Management</h2>
        </div>
        <div className="inline-flex rounded-button border border-border bg-bg-secondary p-1">
          <button
            onClick={() => setActiveTab('files')}
            className={`px-3 py-1.5 rounded-button text-xs font-medium transition-colors ${
              activeTab === 'files'
                ? 'bg-accent text-white'
                : 'text-text-secondary hover:bg-bg-tertiary'
            }`}
          >
            Files
          </button>
          <button
            onClick={() => setActiveTab('index')}
            className={`px-3 py-1.5 rounded-button text-xs font-medium transition-colors ${
              activeTab === 'index'
                ? 'bg-accent text-white'
                : 'text-text-secondary hover:bg-bg-tertiary'
            }`}
          >
            Index
          </button>
        </div>
      </div>

      <p className="px-6 text-xs text-text-muted pb-3 shrink-0">
        {activeTab === 'files'
          ? '浏览和编辑工作区文件系统。左侧为目录树，右侧为文件预览和编辑。'
          : '查看文件索引状态，刷新索引，并通过关键词快速定位工作区文本文件。'}
      </p>

      {/* Main Content */}
      {activeTab === 'files' ? (
      <div className="flex-1 flex overflow-hidden px-6 pb-4">
        {/* Left - Directory Tree */}
        <div className="w-56 border border-border rounded-l-card bg-bg-secondary overflow-hidden flex flex-col shrink-0">
          <div className="flex items-center justify-between px-4 py-3 border-b border-border shrink-0">
            <div>
              <div className="text-xs font-semibold text-text-primary">目录树</div>
              <div className="text-[11px] text-text-muted">浏览工作区树</div>
            </div>
            <button
              onClick={loadTree}
              className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
            >
              <RotateCcw className="w-3 h-3" />
              <span>刷新</span>
            </button>
          </div>
          <div className="flex-1 overflow-y-auto p-2">
            {loading ? (
              <div className="flex items-center justify-center py-8 text-text-muted">
                <Loader2 className="w-4 h-4 animate-spin mr-2" />
                <span className="text-xs">加载中...</span>
              </div>
            ) : tree ? (
              <TreeItem
                node={tree}
                level={0}
                selectedPath={selectedPath}
                onSelect={handleSelect}
                expandedPaths={expandedPaths}
                onToggleExpand={handleToggleExpand}
              />
            ) : (
              <p className="text-xs text-text-muted text-center py-8">暂无数据</p>
            )}
          </div>
        </div>

        {/* Right - File Preview / Editor */}
        <div className="flex-1 flex flex-col min-w-0 border border-l-0 border-border rounded-r-card bg-bg-secondary overflow-hidden">
          {selectedPath && selectedType === 'file' ? (
            <>
              {/* File header */}
              <div className="flex items-center justify-between px-4 py-2.5 border-b border-border shrink-0">
                <div className="flex items-center gap-2">
                  <FileText className="w-4 h-4 text-accent" />
                  <span className="text-sm font-medium text-text-primary">{selectedPath}</span>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={handleReset}
                    className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
                    title="重置"
                  >
                    <RotateCcw className="w-3 h-3" />
                    <span>重置</span>
                  </button>
                  <button
                    onClick={handleSave}
                    disabled={fileLoading || saving}
                    className="flex items-center gap-1 px-3 py-1.5 rounded-button bg-accent hover:bg-accent-hover text-white text-xs font-medium transition-colors disabled:opacity-40"
                  >
                    {saving ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : (
                      <Save className="w-3 h-3" />
                    )}
                    <span>保存</span>
                  </button>
                </div>
              </div>

              {/* Editor */}
              <div className="flex-1 overflow-hidden">
                {fileLoading ? (
                  <div className="flex items-center justify-center h-full text-text-muted">
                    <Loader2 className="w-5 h-5 animate-spin mr-2" />
                    <span className="text-sm">加载中...</span>
                  </div>
                ) : (
                  <textarea
                    value={fileContent}
                    onChange={(e) => setFileContent(e.target.value)}
                    className="w-full h-full p-4 bg-transparent text-sm text-text-primary leading-relaxed outline-none resize-none font-mono"
                    spellCheck={false}
                  />
                )}
              </div>

              {/* Footer status */}
              <div className="flex items-center justify-end px-4 py-2 border-t border-border shrink-0">
                {saveStatus === 'success' && (
                  <span className="text-xs text-status-success">保存成功</span>
                )}
                {saveStatus === 'error' && (
                  <span className="text-xs text-status-error">保存失败</span>
                )}
              </div>
            </>
          ) : selectedPath && selectedType === 'dir' ? (
            <div className="flex-1 flex items-center justify-center text-text-muted">
              <div className="text-center">
                <Folder className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">已选择目录: {selectedPath}</p>
              </div>
            </div>
          ) : (
            <div className="flex-1 flex items-center justify-center text-text-muted">
              <div className="text-center">
                <FileText className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">请选择一个文件进行查看或编辑</p>
              </div>
            </div>
          )}
        </div>
      </div>
      ) : (
      <div className="flex-1 flex overflow-hidden px-6 pb-4">
        <div className="w-80 border border-border rounded-l-card bg-bg-secondary overflow-hidden flex flex-col shrink-0">
          <div className="px-4 py-3 border-b border-border shrink-0">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2 text-xs font-semibold text-text-primary">
                  <Database className="w-3.5 h-3.5 text-accent" />
                  <span>索引状态</span>
                </div>
                <div className="text-[11px] text-text-muted mt-1">
                  {indexStats?.exists ? `最近更新 ${formatTime(indexStats.last_indexed_at)}` : '尚未构建索引'}
                </div>
              </div>
              <button
                onClick={handleRefreshIndex}
                disabled={indexRefreshing}
                className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40"
              >
                {indexRefreshing ? (
                  <Loader2 className="w-3 h-3 animate-spin" />
                ) : (
                  <RotateCcw className="w-3 h-3" />
                )}
                <span>刷新</span>
              </button>
            </div>
            <div className="grid grid-cols-2 gap-2 mt-3">
              <div className="rounded-button bg-bg-tertiary px-3 py-2">
                <div className="text-[11px] text-text-muted">文件数</div>
                <div className="text-sm font-semibold text-text-primary">
                  {indexLoading && !indexStats ? '...' : indexStats?.file_count ?? 0}
                </div>
              </div>
              <div className="rounded-button bg-bg-tertiary px-3 py-2">
                <div className="text-[11px] text-text-muted">内容量</div>
                <div className="text-sm font-semibold text-text-primary">
                  {formatBytes(indexStats?.bytes)}
                </div>
              </div>
            </div>
            {lastRefresh && (
              <div className="mt-3 rounded-button border border-border px-3 py-2 text-[11px] text-text-muted">
                扫描 {lastRefresh.scanned}，新增 {lastRefresh.indexed}，更新 {lastRefresh.updated}，跳过{' '}
                {Object.values(lastRefresh.skipped || {}).reduce((sum, value) => sum + value, 0)}
              </div>
            )}
          </div>

          <div className="p-4 border-b border-border">
            <label className="text-[11px] font-semibold text-text-muted uppercase">Search</label>
            <div className="flex items-center gap-2 mt-2">
              <div className="flex-1 flex items-center gap-2 px-3 py-2 rounded-button border border-border bg-bg-primary">
                <Search className="w-4 h-4 text-text-muted shrink-0" />
                <input
                  value={indexQuery}
                  onChange={(event) => setIndexQuery(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') handleSearchIndex(false);
                  }}
                  placeholder="keyword / path / symbol"
                  className="w-full bg-transparent outline-none text-sm text-text-primary placeholder:text-text-muted"
                />
              </div>
              <button
                onClick={() => handleSearchIndex(false)}
                disabled={indexLoading || !indexQuery.trim()}
                className="px-3 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-xs font-medium transition-colors disabled:opacity-40"
              >
                搜索
              </button>
            </div>
            <div className="flex items-center justify-between gap-3 mt-3">
              <label className="flex items-center gap-2 text-xs text-text-secondary">
                <input
                  type="checkbox"
                  checked={indexPathOnly}
                  onChange={(event) => setIndexPathOnly(event.target.checked)}
                  className="accent-accent"
                />
                仅路径
              </label>
              <select
                value={indexLimit}
                onChange={(event) => setIndexLimit(Number(event.target.value))}
                className="px-2 py-1.5 bg-bg-primary border border-border rounded-button text-xs text-text-secondary outline-none"
              >
                <option value={10}>10 results</option>
                <option value={20}>20 results</option>
                <option value={50}>50 results</option>
              </select>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto p-2">
            {indexError && (
              <div className="m-2 rounded-button border border-status-error/30 bg-status-error/10 px-3 py-2 text-xs text-status-error">
                {indexError}
              </div>
            )}
            {indexLoading && !indexResult ? (
              <div className="flex items-center justify-center py-8 text-text-muted">
                <Loader2 className="w-4 h-4 animate-spin mr-2" />
                <span className="text-xs">检索中...</span>
              </div>
            ) : indexResult && indexResult.matches.length > 0 ? (
              <div className="space-y-1.5">
                {indexResult.matches.map((match) => (
                  <button
                    key={`${match.path}:${match.match_type}:${match.line ?? 0}`}
                    onClick={() => handlePreviewMatch(match)}
                    className="w-full text-left rounded-button border border-transparent hover:border-border hover:bg-bg-tertiary px-3 py-2 transition-colors"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate text-xs font-medium text-text-primary">{match.path}</span>
                      <span className="shrink-0 rounded-button bg-accent/10 px-1.5 py-0.5 text-[10px] text-accent">
                        {match.match_type}
                      </span>
                    </div>
                    <div className="mt-1 text-[11px] text-text-muted">
                      {match.line ? `L${match.line} · ` : ''}{formatBytes(match.size_bytes)}
                    </div>
                    <div className="mt-1 line-clamp-2 text-xs text-text-secondary">{match.snippet || '(no preview)'}</div>
                  </button>
                ))}
              </div>
            ) : indexResult ? (
              <div className="py-10 text-center text-xs text-text-muted">没有匹配结果</div>
            ) : (
              <div className="py-10 text-center text-xs text-text-muted">
                输入关键词后搜索索引
              </div>
            )}
          </div>
        </div>

        <div className="flex-1 flex flex-col min-w-0 border border-l-0 border-border rounded-r-card bg-bg-secondary overflow-hidden">
          {previewLoading ? (
            <div className="flex-1 flex items-center justify-center text-text-muted">
              <Loader2 className="w-5 h-5 animate-spin mr-2" />
              <span className="text-sm">加载预览...</span>
            </div>
          ) : previewFile ? (
            <>
              <div className="flex items-center justify-between px-4 py-2.5 border-b border-border shrink-0">
                <div className="flex items-center gap-2 min-w-0">
                  <Eye className="w-4 h-4 text-accent shrink-0" />
                  <span className="truncate text-sm font-medium text-text-primary">{previewFile.path}</span>
                </div>
                <span className="shrink-0 text-xs text-text-muted">{formatBytes(previewFile.size_bytes)} · read-only</span>
              </div>
              {previewFile.path.startsWith('system/') && (
                <div className="border-b border-border px-4 py-2 text-xs text-text-muted">
                  该文件位于 system，可在 Files 视图中编辑。
                </div>
              )}
              <pre className="flex-1 overflow-auto p-4 text-sm leading-relaxed text-text-primary font-mono whitespace-pre-wrap">
                {previewFile.content}
              </pre>
            </>
          ) : previewError ? (
            <div className="flex-1 flex items-center justify-center text-status-error">
              <p className="text-sm">{previewError}</p>
            </div>
          ) : (
            <div className="flex-1 flex items-center justify-center text-text-muted">
              <div className="text-center">
                <FileSearch className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">选择搜索结果进行只读预览</p>
              </div>
            </div>
          )}
        </div>
      </div>
      )}
    </div>
  );
}
