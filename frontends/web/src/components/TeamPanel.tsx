import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ArrowLeft,
  Bot,
  Check,
  FolderOpen,
  Loader2,
  MessageSquare,
  Plus,
  RefreshCw,
  Save,
  Search,
  Trash2,
  Users,
} from 'lucide-react';
import { api } from '../api/client';
import type { AgentTeam, AgentTeamMember, WorkspaceAgent } from '../types';

interface TeamPanelProps {
  workspace: string;
  onSelectTeam?: (teamName: string) => void;
  onCreateTeam?: () => void;
  onChatWithTeam?: (team: AgentTeam) => void;
}

interface TeamDetailProps {
  workspace: string;
  teamName: string | null;
  onBack: () => void;
  onChatWithTeam?: (team: AgentTeam) => void;
}

const emptyTeam = (leader = 'main'): AgentTeam => ({
  name: '',
  description: '',
  leader,
  mode: 'leader_delegates',
  members: [],
});

export function TeamPanel({ workspace, onSelectTeam, onCreateTeam, onChatWithTeam }: TeamPanelProps) {
  const [teams, setTeams] = useState<AgentTeam[]>([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');

  const loadTeams = useCallback(() => {
    if (!workspace) return;
    setLoading(true);
    api.listTeams(workspace)
      .then(res => {
        if (res.success && res.data) setTeams(res.data);
        else setTeams([]);
      })
      .finally(() => setLoading(false));
  }, [workspace]);

  useEffect(() => {
    loadTeams();
  }, [loadTeams]);

  const filteredTeams = useMemo(() => {
    if (!search.trim()) return teams;
    const q = search.toLowerCase();
    return teams.filter(team =>
      team.name.toLowerCase().includes(q)
      || team.description.toLowerCase().includes(q)
      || team.leader.toLowerCase().includes(q)
    );
  }, [search, teams]);

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-border shrink-0">
        <h2 className="text-xl font-bold text-text-primary">Teams</h2>
      </div>

      <div className="px-6 pt-4 pb-2 shrink-0">
        <p className="text-sm text-text-secondary leading-relaxed">
          组合多个已有 Agent，由 leader 串行委派子任务，适合研究、实现、审查等多角色工作流。
        </p>
        <div className="flex items-center gap-1.5 mt-2 text-xs text-text-muted">
          <FolderOpen className="w-3.5 h-3.5" />
          <span className="font-mono">workspace/{workspace}/system/teams</span>
        </div>
      </div>

      <div className="flex items-center justify-between px-6 py-3 gap-3 shrink-0">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-muted" />
          <input
            type="text"
            value={search}
            onChange={event => setSearch(event.target.value)}
            placeholder="搜索 Team 名"
            className="w-full pl-8 pr-3 py-2 bg-bg-tertiary border border-border rounded-input text-sm text-text-primary placeholder-text-muted outline-none focus:border-accent/50 transition-colors"
          />
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={loadTeams}
            className="flex items-center gap-1.5 px-3 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            <span>刷新</span>
          </button>
          <button
            onClick={onCreateTeam}
            className="flex items-center gap-1.5 px-3 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>新增 Team</span>
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-4">
        {loading && (
          <div className="flex items-center justify-center py-12 text-text-muted">
            <Loader2 className="w-5 h-5 animate-spin mr-2" />
            <span className="text-sm">加载中...</span>
          </div>
        )}

        {!loading && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {filteredTeams.map(team => (
              <div
                key={team.name}
                onClick={() => onSelectTeam?.(team.name)}
                className="overflow-hidden bg-bg-secondary border border-border rounded-card p-4 hover:shadow-lg hover:-translate-y-0.5 hover:border-accent/20 transition-all duration-200 cursor-pointer"
              >
                <div className="flex items-center gap-3">
                  <div className="w-9 h-9 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
                    <Users className="w-4 h-4 text-accent" />
                  </div>
                  <h3 className="text-base font-semibold text-text-primary truncate">
                    {team.name}
                  </h3>
                </div>

                <p className="text-sm text-text-secondary mt-3 leading-relaxed line-clamp-3">
                  {team.description || '未配置描述'}
                </p>

                <div className="mt-4 pt-3 border-t border-border space-y-3">
                  <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-2 text-xs text-text-muted">
                    <span className="flex items-center gap-1" title="Leader">
                      <Bot className="w-4 h-4" />
                      <span className="font-medium">{team.leader || 'main'}</span>
                    </span>
                    <span className="flex items-center gap-1" title="Members">
                      <Users className="w-4 h-4" />
                      <span className="font-medium">{team.members.length}</span>
                    </span>
                    <span className="max-w-full truncate font-mono">{team.mode}</span>
                  </div>
                  <button
                    onClick={event => {
                      event.stopPropagation();
                      onChatWithTeam?.(team);
                    }}
                    className="flex w-full items-center justify-center gap-1.5 px-3 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary hover:border-accent/30 transition-colors"
                  >
                    <MessageSquare className="w-3 h-3" />
                    <span>对话</span>
                  </button>
                </div>
              </div>
            ))}
            {filteredTeams.length === 0 && (
              <div className="col-span-full text-center py-12 text-text-muted">
                <Users className="w-10 h-10 mx-auto mb-3 opacity-20" />
                <p className="text-sm">未找到 Team</p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export function TeamDetail({ workspace, teamName, onBack, onChatWithTeam }: TeamDetailProps) {
  const [team, setTeam] = useState<AgentTeam>(emptyTeam());
  const [agents, setAgents] = useState<WorkspaceAgent[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [changed, setChanged] = useState(false);
  const [error, setError] = useState('');

  const agentNames = useMemo(() => agents.map(agent => agent.name), [agents]);

  const loadDetail = useCallback(async () => {
    setLoading(true);
    setError('');
    const [agentsRes, teamRes] = await Promise.all([
      api.listAgents(workspace),
      teamName ? api.readTeam(workspace, teamName) : Promise.resolve(null),
    ]);

    const nextAgents = agentsRes.success && agentsRes.data ? agentsRes.data : [];
    setAgents(nextAgents);
    const fallbackLeader = nextAgents.some(agent => agent.name === 'main') ? 'main' : nextAgents[0]?.name || 'main';

    if (teamRes && teamRes.success && teamRes.data) {
      setTeam(teamRes.data);
    } else {
      setTeam(emptyTeam(fallbackLeader));
      if (teamRes && !teamRes.success) setError(teamRes.error || 'Failed to load team');
    }
    setChanged(false);
    setLoading(false);
  }, [teamName, workspace]);

  useEffect(() => {
    loadDetail();
  }, [loadDetail]);

  const updateTeam = (patch: Partial<AgentTeam>) => {
    setTeam(current => ({ ...current, ...patch }));
    setChanged(true);
  };

  const updateMember = (index: number, patch: Partial<AgentTeamMember>) => {
    setTeam(current => ({
      ...current,
      members: current.members.map((member, i) => i === index ? { ...member, ...patch } : member),
    }));
    setChanged(true);
  };

  const addMember = () => {
    const existing = new Set(team.members.map(member => member.agent));
    const candidate = agentNames.find(name => name !== team.leader && !existing.has(name)) || '';
    setTeam(current => ({
      ...current,
      members: [...current.members, { agent: candidate, role: '', autoDelegate: true }],
    }));
    setChanged(true);
  };

  const removeMember = (index: number) => {
    setTeam(current => ({
      ...current,
      members: current.members.filter((_, i) => i !== index),
    }));
    setChanged(true);
  };

  const saveTeam = async () => {
    setSaving(true);
    setError('');
    setSaveStatus('idle');
    const res = await api.writeTeam(workspace, team);
    setSaving(false);
    if (!res.success || !res.data) {
      setSaveStatus('error');
      setError(res.error || 'Failed to save team');
      return;
    }
    setTeam(res.data);
    setChanged(false);
    setSaveStatus('success');
    setTimeout(() => setSaveStatus('idle'), 2000);
  };

  const deleteTeam = async () => {
    if (!team.name || !window.confirm(`Delete team ${team.name}?`)) return;
    const res = await api.deleteTeam(workspace, team.name);
    if (!res.success) {
      setError(res.error || 'Failed to delete team');
      return;
    }
    onBack();
  };

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden bg-bg-primary">
      <div className="flex items-center justify-between px-6 py-4 border-b border-border shrink-0">
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="flex items-center gap-1.5 text-sm text-text-muted hover:text-text-secondary transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            <span>返回</span>
          </button>
          <div className="w-px h-5 bg-border" />
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-accent/10 flex items-center justify-center">
              <Users className="w-4 h-4 text-accent" />
            </div>
            <div>
              <h2 className="text-base font-semibold text-text-primary">{team.name || 'New Team'}</h2>
              <p className="text-xs text-text-muted font-mono">
                system/teams/{team.name || '<new>'}.json
              </p>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {team.name && (
            <button
              onClick={() => onChatWithTeam?.(team)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-button border border-border text-xs text-text-secondary hover:bg-bg-tertiary transition-colors"
            >
              <MessageSquare className="w-3 h-3" />
              <span>对话</span>
            </button>
          )}
          {teamName && (
            <button
              onClick={deleteTeam}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-button border border-status-error/30 text-xs text-status-error hover:bg-status-error/10 transition-colors"
            >
              <Trash2 className="w-3 h-3" />
              <span>删除</span>
            </button>
          )}
          <button
            onClick={saveTeam}
            disabled={saving || loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-button bg-accent hover:bg-accent-hover text-white text-xs font-medium transition-colors disabled:opacity-50"
          >
            {saving ? (
              <Loader2 className="w-3 h-3 animate-spin" />
            ) : saveStatus === 'success' ? (
              <Check className="w-3 h-3" />
            ) : (
              <Save className="w-3 h-3" />
            )}
            <span>{saving ? '保存中' : saveStatus === 'success' ? '已保存' : '保存'}</span>
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-5">
        {loading ? (
          <div className="flex items-center justify-center py-12 text-text-muted">
            <Loader2 className="w-5 h-5 animate-spin mr-2" />
            <span className="text-sm">加载中...</span>
          </div>
        ) : (
          <>
            <div className="grid max-w-5xl grid-cols-1 gap-4 lg:grid-cols-2">
              <label className="block">
                <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-text-muted">Name</span>
                <input
                  value={team.name}
                  onChange={event => updateTeam({ name: event.target.value })}
                  placeholder="deepresearch"
                  className="w-full rounded-input border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                />
              </label>
              <label className="block">
                <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-text-muted">Leader</span>
                <select
                  value={team.leader}
                  onChange={event => updateTeam({ leader: event.target.value })}
                  className="w-full rounded-input border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                >
                  {agentNames.map(name => <option key={name} value={name}>{name}</option>)}
                </select>
              </label>
              <label className="block lg:col-span-2">
                <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-text-muted">Description</span>
                <input
                  value={team.description}
                  onChange={event => updateTeam({ description: event.target.value })}
                  placeholder="Research team for source discovery, analysis, writing, and review"
                  className="w-full rounded-input border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                />
              </label>
              <label className="block">
                <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-text-muted">Mode</span>
                <select
                  value={team.mode}
                  onChange={event => updateTeam({ mode: event.target.value })}
                  className="w-full rounded-input border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                >
                  <option value="leader_delegates">leader_delegates</option>
                  <option value="manual">manual</option>
                  <option value="roundtable_review">roundtable_review</option>
                </select>
              </label>
            </div>

            <div className="mt-6 max-w-5xl">
              <div className="mb-3 flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-text-primary">Members</h3>
                  <p className="mt-0.5 text-xs text-text-muted">成员引用已有 Agent 配置，不复制 AGENT.md。</p>
                </div>
                <button
                  onClick={addMember}
                  className="flex items-center gap-1.5 rounded-button border border-border px-3 py-2 text-sm text-text-secondary hover:bg-bg-tertiary"
                >
                  <Plus className="h-4 w-4" />
                  Add member
                </button>
              </div>
              <div className="space-y-2">
                {team.members.map((member, index) => (
                  <div key={`${member.agent}-${index}`} className="grid grid-cols-12 gap-2 rounded-card border border-border bg-bg-secondary p-3">
                    <select
                      value={member.agent}
                      onChange={event => updateMember(index, { agent: event.target.value })}
                      className="col-span-12 sm:col-span-3 rounded-input border border-border bg-bg-primary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                    >
                      <option value="">Select agent</option>
                      {agentNames.map(name => <option key={name} value={name}>{name}</option>)}
                    </select>
                    <input
                      value={member.role}
                      onChange={event => updateMember(index, { role: event.target.value })}
                      placeholder="role"
                      className="col-span-12 sm:col-span-5 rounded-input border border-border bg-bg-primary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
                    />
                    <label className="col-span-10 sm:col-span-3 flex items-center gap-2 rounded-input border border-border bg-bg-primary px-3 py-2 text-sm text-text-secondary">
                      <input
                        type="checkbox"
                        checked={member.autoDelegate}
                        onChange={event => updateMember(index, { autoDelegate: event.target.checked })}
                      />
                      Auto delegate
                    </label>
                    <button
                      onClick={() => removeMember(index)}
                      className="col-span-2 sm:col-span-1 flex items-center justify-center rounded-button text-text-muted hover:bg-bg-tertiary hover:text-status-error"
                      title="Remove member"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                ))}
                {team.members.length === 0 && (
                  <div className="rounded-card border border-dashed border-border px-4 py-8 text-center text-sm text-text-muted">
                    Add at least one member to enable delegation.
                  </div>
                )}
              </div>
            </div>
            {changed && (
              <div className="mt-4 text-xs text-text-muted">有未保存的更改</div>
            )}
            {error && (
              <div className="mt-4 max-w-5xl rounded-input border border-status-error/30 bg-status-error/10 px-3 py-2 text-sm text-status-error">
                {error}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
