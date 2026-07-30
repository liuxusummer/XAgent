import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Bot,
  Zap,
  FileText,
  Loader2,
  Search,
  Plus,
  RefreshCw,
  MessageSquare,
  FolderOpen,
  Wrench,
  Hammer,
  Settings,
} from 'lucide-react';
import { api } from '../api/client';
import type { WorkspaceAgent, WorkspaceSkill } from '../types';

export type PanelTab = 'agents' | 'skills';

interface WorkspacePanelProps {
  isOpen: boolean;
  workspace: string;
  defaultTab?: PanelTab;
  onSelectAgent?: (agentName: string) => void;
  onChatWithAgent?: (agentName: string) => void;
}

export function WorkspacePanel({
  isOpen,
  workspace,
  defaultTab = 'agents',
  onSelectAgent,
  onChatWithAgent,
}: WorkspacePanelProps) {
  const [activeTab, setActiveTab] = useState<PanelTab>(defaultTab);
  const [agents, setAgents] = useState<WorkspaceAgent[]>([]);
  const [skills, setSkills] = useState<WorkspaceSkill[]>([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');

  const loadData = useCallback(() => {
    if (!workspace) return;
    setLoading(true);
    Promise.all([
      api.listAgents(workspace).then((res) => {
        if (res.success && res.data) setAgents(res.data);
        else setAgents([]);
      }),
      api.listSkills(workspace).then((res) => {
        if (res.success && res.data) setSkills(res.data);
        else setSkills([]);
      }),
    ]).finally(() => setLoading(false));
  }, [workspace]);

  useEffect(() => {
    if (!isOpen) return;
    const timer = window.setTimeout(loadData, 0);
    return () => window.clearTimeout(timer);
  }, [isOpen, loadData]);

  useEffect(() => {
    if (!isOpen || !defaultTab) return;
    const timer = window.setTimeout(() => setActiveTab(defaultTab), 0);
    return () => window.clearTimeout(timer);
  }, [isOpen, defaultTab]);

  const filteredAgents = useMemo(() => {
    if (!search.trim()) return agents;
    const q = search.toLowerCase();
    return agents.filter((a) => a.name.toLowerCase().includes(q));
  }, [agents, search]);

  const filteredSkills = useMemo(() => {
    if (!search.trim()) return skills;
    const q = search.toLowerCase();
    return skills.filter((s) => s.name.toLowerCase().includes(q));
  }, [skills, search]);



  if (!isOpen) return null;

  const isAgents = activeTab === 'agents';
  const title = isAgents ? 'Agent' : 'Skill';
  const description = isAgents
    ? '支持云端与本地的开发与调试，集成各类 Skill、变量管理与安全脱敏，为您提供安全高效的智能体开发与运行环境。'
    : '管理工作区技能资源，集成各类工具能力，为 Agent 提供可复用的功能模块。';
  const pathLabel = isAgents
    ? `workspace/${workspace}/system/agents`
    : `workspace/${workspace}/system/skills`;

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-border shrink-0">
        <h2 className="text-xl font-bold text-text-primary">{title}</h2>
      </div>

      {/* Intro */}
      <div className="px-6 pt-4 pb-2 shrink-0">
        <p className="text-sm text-text-secondary leading-relaxed">{description}</p>
        <div className="flex items-center gap-1.5 mt-2 text-xs text-text-muted">
          <FolderOpen className="w-3.5 h-3.5" />
          <span className="font-mono">{pathLabel}</span>
        </div>
      </div>

      {/* Toolbar */}
      <div className="flex items-center justify-between px-6 py-3 gap-3 shrink-0">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-muted" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={`搜索 ${title} 名`}
            className="w-full pl-8 pr-3 py-2 bg-bg-tertiary border border-border rounded-input text-sm text-text-primary placeholder-text-muted outline-none focus:border-accent/50 transition-colors"
          />
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadData}
            className="flex items-center gap-1.5 px-3 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            <span>刷新</span>
          </button>
          <button className="flex items-center gap-1.5 px-3 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors">
            <Plus className="w-3.5 h-3.5" />
            <span>新增 {title}</span>
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto px-6 py-4">
        {loading && (
          <div className="flex items-center justify-center py-12 text-text-muted">
            <Loader2 className="w-5 h-5 animate-spin mr-2" />
            <span className="text-sm">加载中...</span>
          </div>
        )}

        {!loading && activeTab === 'agents' && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {filteredAgents.map((agent) => (
              <div
                key={agent.name}
                onClick={() => onSelectAgent?.(agent.name)}
                className="bg-bg-secondary border border-border rounded-card p-4 hover:shadow-lg hover:-translate-y-0.5 hover:border-accent/20 transition-all duration-200 cursor-pointer"
              >
                <div className="flex items-center gap-3">
                  <div className="w-9 h-9 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
                    <span className="text-sm font-bold text-accent">
                      {agent.name.charAt(0).toUpperCase()}
                    </span>
                  </div>
                  <h3 className="text-base font-semibold text-text-primary truncate">
                    {agent.name}
                  </h3>
                </div>

                {/* Description */}
                <p className="text-sm text-text-secondary mt-3 leading-relaxed">
                  {agent.description || '未配置描述'}
                </p>

                {/* Bottom row: icons + chat button */}
                <div className="flex items-center justify-between mt-4 pt-3 border-t border-border">
                  <div className="flex items-center gap-3">
                    <span
                      title={`${agent.files.length} 个文件`}
                      className="flex items-center gap-1 text-xs text-text-muted"
                    >
                      <FileText className="w-4 h-4" />
                      <span className="font-medium">{agent.files.length}</span>
                    </span>
                    <span
                      title="已配置技能"
                      className="flex items-center gap-1 text-xs text-text-muted"
                    >
                      <Wrench className="w-4 h-4" />
                    </span>
                    <span
                      title="已配置工具"
                      className="flex items-center gap-1 text-xs text-text-muted"
                    >
                      <Hammer className="w-4 h-4" />
                    </span>
                    <span
                      title="已配置参数"
                      className="flex items-center gap-1 text-xs text-text-muted"
                    >
                      <Settings className="w-4 h-4" />
                    </span>
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      onChatWithAgent?.(agent.name);
                    }}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary hover:border-accent/30 transition-colors"
                  >
                    <MessageSquare className="w-3 h-3" />
                    <span>对话</span>
                  </button>
                </div>
              </div>
            ))}
            {filteredAgents.length === 0 && !loading && (
              <div className="col-span-full text-center py-12 text-text-muted">
                <Bot className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">未找到 Agent</p>
              </div>
            )}
          </div>
        )}

        {!loading && activeTab === 'skills' && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {filteredSkills.map((skill) => (
              <div
                key={skill.name}
                className="bg-bg-secondary border border-border rounded-card p-4 hover:shadow-lg hover:-translate-y-0.5 hover:border-accent/20 transition-all duration-200 cursor-pointer"
              >
                <div className="flex items-start gap-3">
                  <div className="w-10 h-10 rounded-xl bg-accent/10 flex items-center justify-center shrink-0">
                    <Zap className="w-4 h-4 text-accent" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="text-sm font-semibold text-text-primary truncate">
                      {skill.name}
                    </h3>
                    <p className="text-xs text-text-muted mt-0.5">工作区技能</p>
                  </div>
                </div>
              </div>
            ))}
            {filteredSkills.length === 0 && !loading && (
              <div className="col-span-full text-center py-12 text-text-muted">
                <Zap className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">未找到 Skill</p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
