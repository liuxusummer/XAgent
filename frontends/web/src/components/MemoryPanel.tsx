import { useState, useEffect, useCallback } from 'react';
import {
  Loader2,
  Save,
  FileText,
  ChevronDown,
  Trash2,
  Download,
  RotateCcw,
} from 'lucide-react';
import { api } from '../api/client';
import type { WorkspaceAgent } from '../types';

interface MemoryPanelProps {
  workspace: string;
  agentName?: string | null;
}

export function MemoryPanel({ workspace, agentName }: MemoryPanelProps) {
  const [agents, setAgents] = useState<WorkspaceAgent[]>([]);
  const [selectedAgent, setSelectedAgent] = useState<string>(agentName || 'global');
  const [files, setFiles] = useState<string[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [content, setContent] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [statusMessage, setStatusMessage] = useState('');

  // Load agents list
  useEffect(() => {
    api.listAgents(workspace).then((res) => {
      if (res.success && res.data) {
        setAgents(res.data);
      }
    });
  }, [workspace]);

  // Update selectedAgent when agentName prop changes
  useEffect(() => {
    if (agentName) {
      const timer = window.setTimeout(() => setSelectedAgent(agentName), 0);
      return () => window.clearTimeout(timer);
    }
  }, [agentName]);

  // Load files based on selected agent
  useEffect(() => {
    const timer = window.setTimeout(() => {
      if (selectedAgent === 'global') {
        setFiles(['project.md']);
        setSelectedFile('project.md');
      } else {
        api.listAgents(workspace).then((res) => {
          if (res.success && res.data) {
            const agent = res.data.find((a) => a.name === selectedAgent);
            if (agent) {
              const memoryFiles = agent.files.filter((f) =>
                f.toLowerCase().includes('memory')
              );
              setFiles(memoryFiles.length > 0 ? memoryFiles : ['MEMORY.md']);
              setSelectedFile(memoryFiles.length > 0 ? memoryFiles[0] : 'MEMORY.md');
            }
          }
        });
      }
    }, 0);
    return () => window.clearTimeout(timer);
  }, [selectedAgent, workspace]);

  // Load file content
  const loadContent = useCallback(() => {
    if (!workspace || !selectedFile) return;
    const path =
      selectedAgent === 'global'
        ? `system/memory/${selectedFile}`
        : `system/agents/${selectedAgent}/${selectedFile}`;
    setLoading(true);
    api.readWorkspaceFile(workspace, path)
      .then((res) => {
        if (res.success && res.data) {
          setContent(res.data.content);
        } else {
          setContent('');
        }
      })
      .catch(() => setContent(''))
      .finally(() => setLoading(false));
  }, [workspace, selectedAgent, selectedFile]);

  useEffect(() => {
    const timer = window.setTimeout(loadContent, 0);
    return () => window.clearTimeout(timer);
  }, [loadContent]);

  const handleSave = async () => {
    if (!selectedFile) return;
    const path =
      selectedAgent === 'global'
        ? `system/memory/${selectedFile}`
        : `system/agents/${selectedAgent}/${selectedFile}`;
    setSaving(true);
    setSaveStatus('idle');
    setStatusMessage('');
    const res = await api.writeWorkspaceFile(workspace, path, content);
    if (res.success) {
      setSaveStatus('success');
      setStatusMessage('保存成功');
      setTimeout(() => {
        setSaveStatus('idle');
        setStatusMessage('');
      }, 2000);
    } else {
      setSaveStatus('error');
      setStatusMessage(res.error || '保存失败');
    }
    setSaving(false);
  };

  const handleDelete = async () => {
    if (!selectedFile) return;
    const path =
      selectedAgent === 'global'
        ? `system/memory/${selectedFile}`
        : `system/agents/${selectedAgent}/${selectedFile}`;
    setSaveStatus('idle');
    setStatusMessage('');
    const res = await api.deleteWorkspaceFile(workspace, path);
    if (res.success) {
      const remainingFiles = files.filter((file) => file !== selectedFile);
      setFiles(remainingFiles);
      setSelectedFile(remainingFiles[0] || null);
      setContent('');
      setSaveStatus('success');
      setStatusMessage('删除成功');
    } else {
      setSaveStatus('error');
      setStatusMessage(res.error || '删除失败');
    }
  };

  const handleDownload = () => {
    if (!content) return;
    const blob = new Blob([content], { type: 'text/markdown' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = selectedFile || 'memory.md';
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleReset = () => {
    loadContent();
  };

  const isGlobal = selectedAgent === 'global';
  const agentDisplay = isGlobal ? '全局记忆' : `Agent ${selectedAgent}`;

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden bg-bg-primary">
      {/* Top Bar */}
      <div className="flex items-start justify-between px-6 pt-5 pb-3 shrink-0">
        <div>
          <h2 className="text-lg font-bold text-text-primary">Memory</h2>
          <p className="text-xs text-text-muted mt-1">
            这里编辑的是 {agentDisplay} 的专属记忆，适合放该 Agent 独有的事实、约束、工作偏好和阶段性上下文。
          </p>
        </div>
        <div className="flex flex-col items-end gap-1.5">
          <label className="text-xs font-medium text-text-muted">对象</label>
          <div className="relative w-48">
            <select
              value={selectedAgent}
              onChange={(e) => setSelectedAgent(e.target.value)}
              className="w-full px-3 py-2 bg-bg-secondary border border-border rounded-card text-sm text-text-primary outline-none focus:border-accent/50 transition-colors appearance-none cursor-pointer"
            >
              <option value="global">全局记忆</option>
              {agents.map((agent) => (
                <option key={agent.name} value={agent.name}>
                  {agent.name}
                </option>
              ))}
            </select>
            <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-text-muted pointer-events-none" />
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden px-6 pb-4">
        {/* Sidebar - File List */}
        <div className="w-52 border border-border rounded-l-card bg-bg-secondary overflow-hidden flex flex-col shrink-0">
          <div className="px-4 py-3 text-xs font-semibold text-text-primary border-b border-border">
            Agent Memory
          </div>
          <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
            {files.map((file) => (
              <button
                key={file}
                onClick={() => setSelectedFile(file)}
                className={`flex items-center gap-2 w-full px-3 py-2 rounded-button text-sm transition-colors ${
                  selectedFile === file
                    ? 'bg-accent/10 text-accent'
                    : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
                }`}
              >
                <FileText className="w-3.5 h-3.5 shrink-0" />
                <span className="truncate">{file}</span>
              </button>
            ))}
            {files.length === 0 && (
              <p className="px-3 py-2 text-xs text-text-muted">暂无记忆文件</p>
            )}
          </div>
        </div>

        {/* Editor Area */}
        <div className="flex-1 flex flex-col min-w-0 border border-l-0 border-border rounded-r-card bg-bg-secondary overflow-hidden">
          {/* File header */}
          <div className="flex items-center justify-between px-4 py-2.5 border-b border-border shrink-0">
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium text-text-primary">{selectedFile}</span>
              <span className="px-1.5 py-0.5 rounded bg-accent/10 text-accent text-xs">
                {isGlobal ? '全局' : '私有'}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={handleReset}
                className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
                title="重置"
              >
                <RotateCcw className="w-3 h-3" />
              </button>
              <button
                onClick={handleDelete}
                className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
                title="删除"
              >
                <Trash2 className="w-3 h-3" />
              </button>
              <button
                onClick={handleDownload}
                className="flex items-center gap-1 px-2.5 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
                title="下载"
              >
                <Download className="w-3 h-3" />
              </button>
              <button
                onClick={handleSave}
                disabled={loading || saving}
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
            {loading ? (
              <div className="flex-1 flex items-center justify-center text-text-muted h-full">
                <Loader2 className="w-5 h-5 animate-spin mr-2" />
                <span className="text-sm">加载中...</span>
              </div>
            ) : (
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                className="w-full h-full p-4 bg-transparent text-sm text-text-primary leading-relaxed outline-none resize-none font-mono"
                spellCheck={false}
              />
            )}
          </div>

          {/* Footer status */}
          <div className="flex items-center justify-end px-4 py-2 border-t border-border shrink-0">
            {saveStatus === 'success' && (
              <span className="text-xs text-status-success">{statusMessage}</span>
            )}
            {saveStatus === 'error' && (
              <span className="text-xs text-status-error">{statusMessage}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
