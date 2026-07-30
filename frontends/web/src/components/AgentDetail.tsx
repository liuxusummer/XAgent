import { useState, useEffect, useCallback } from 'react';
import {
  ArrowLeft,
  Save,
  FileText,
  Bot,
  Loader2,
  Check,
  Wrench,
  Hammer,
  FolderOpen,
  FileCode,
   BarChart3,
   Layers,
   Zap,
   Cpu,
 } from 'lucide-react';
import { api } from '../api/client';
import type { AgentProfile, AgentProfileData, WorkspaceSkill, WorkspaceTool } from '../types';

interface AgentDetailProps {
  workspace: string;
  agentName: string;
  onBack: () => void;
}

type NavTab = 'overview' | 'files' | 'skills' | 'tools' | 'config';

function defaultProfile(agentName: string): AgentProfile {
  return {
    name: agentName,
    description: '',
    tools: [],
    model: '',
    maxTurns: 300,
    memory: '',
    skills: [],
    project_agents: [],
  };
}

function linesToList(value: string): string[] {
  return value
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean);
}

function listToLines(value: string[]): string {
  return value.join('\n');
}

export function AgentDetail({ workspace, agentName, onBack }: AgentDetailProps) {
  const [activeTab, setActiveTab] = useState<NavTab>('files');
  const [files, setFiles] = useState<string[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [fileContent, setFileContent] = useState('');
  const [editedContent, setEditedContent] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [hasChanges, setHasChanges] = useState(false);

  const [profile, setProfile] = useState<AgentProfile>(() => defaultProfile(agentName));
  const [agentBody, setAgentBody] = useState('');
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileSaving, setProfileSaving] = useState(false);
  const [profileChanged, setProfileChanged] = useState(false);
  const [projectAgentsDraft, setProjectAgentsDraft] = useState('');

  const [allSkills, setAllSkills] = useState<WorkspaceSkill[]>([]);
  const [selectedSkills, setSelectedSkills] = useState<Set<string>>(new Set());
  const [skillsLoading, setSkillsLoading] = useState(false);
  const [skillsSaving, setSkillsSaving] = useState(false);
  const [skillsChanged, setSkillsChanged] = useState(false);

  const [allTools, setAllTools] = useState<WorkspaceTool[]>([]);
  const [selectedTools, setSelectedTools] = useState<Set<string>>(new Set());
  const [toolsLoading, setToolsLoading] = useState(false);
  const [toolsSaving, setToolsSaving] = useState(false);
  const [toolsChanged, setToolsChanged] = useState(false);

  const applyProfileData = useCallback(
    (data: AgentProfileData) => {
      setProfile(data.profile);
      setAgentBody(data.body);
      setProjectAgentsDraft(listToLines(data.profile.project_agents || []));
      setSelectedSkills(new Set(data.profile.skills || []));
      setSelectedTools(new Set(data.profile.tools || []));
      setProfileChanged(false);
      setSkillsChanged(false);
      setToolsChanged(false);
    },
    []
  );

  const loadAgentProfile = useCallback(async () => {
    setProfileLoading(true);
    try {
      const res = await api.readAgentProfile(workspace, agentName);
      if (res.success && res.data) {
        applyProfileData(res.data);
      } else {
        const fallback = defaultProfile(agentName);
        setProfile(fallback);
        setAgentBody('');
        setProjectAgentsDraft('');
        setSelectedSkills(new Set());
        setSelectedTools(new Set());
      }
    } finally {
      setProfileLoading(false);
    }
  }, [workspace, agentName, applyProfileData]);

  const loadFiles = useCallback(() => {
    api.listAgents(workspace).then((res) => {
      if (res.success && res.data) {
        const agent = res.data.find((item) => item.name === agentName);
        if (agent) {
          setFiles(agent.files);
          setSelectedFile((current) => current || (agent.files.includes('AGENT.md') ? 'AGENT.md' : agent.files[0] || null));
          if (agent.profile) {
            setProfile(agent.profile);
            setProjectAgentsDraft(listToLines(agent.profile.project_agents || []));
          }
        }
      }
    });
  }, [workspace, agentName]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setProfile(defaultProfile(agentName));
      setAgentBody('');
      setProjectAgentsDraft('');
      setSelectedFile(null);
      setFileContent('');
      setEditedContent('');
      setHasChanges(false);
      loadFiles();
      loadAgentProfile();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [agentName, loadFiles, loadAgentProfile]);

  useEffect(() => {
    if (!selectedFile) return;
    const timer = window.setTimeout(() => {
      setLoading(true);
      const path = `system/agents/${agentName}/${selectedFile}`;
      api.readWorkspaceFile(workspace, path).then((res) => {
        if (res.success && res.data) {
          setFileContent(res.data.content);
          setEditedContent(res.data.content);
          setHasChanges(false);
        }
        setLoading(false);
      });
    }, 0);
    return () => window.clearTimeout(timer);
  }, [workspace, agentName, selectedFile]);

  const saveProfile = useCallback(
    async (nextProfile: AgentProfile, nextBody: string) => {
      const normalizedProfile = { ...nextProfile, name: agentName };
      const res = await api.writeAgentProfile(workspace, agentName, normalizedProfile, nextBody);
      if (res.success && res.data) {
        applyProfileData(res.data);
        if (selectedFile === 'AGENT.md') {
          setFileContent(res.data.content);
          setEditedContent(res.data.content);
          setHasChanges(false);
        }
        loadFiles();
      }
      return res;
    },
    [workspace, agentName, selectedFile, applyProfileData, loadFiles]
  );

  const handleSave = async () => {
    if (!selectedFile) return;
    setSaving(true);
    setSaveStatus('idle');
    const path = `system/agents/${agentName}/${selectedFile}`;
    const res = await api.writeWorkspaceFile(workspace, path, editedContent);
    setSaving(false);
    if (res.success) {
      setSaveStatus('success');
      setFileContent(editedContent);
      setHasChanges(false);
      if (selectedFile === 'AGENT.md') {
        await loadAgentProfile();
      }
      setTimeout(() => setSaveStatus('idle'), 2000);
    } else {
      setSaveStatus('error');
    }
  };

  const handleContentChange = (value: string) => {
    setEditedContent(value);
    setHasChanges(value !== fileContent);
  };

  const loadSkills = useCallback(async () => {
    setSkillsLoading(true);
    try {
      const skillsRes = await api.listSkills(workspace);
      if (skillsRes.success && skillsRes.data) {
        setAllSkills(skillsRes.data);
      } else {
        setAllSkills([]);
      }
      setSelectedSkills(new Set(profile.skills || []));
      setSkillsChanged(false);
    } finally {
      setSkillsLoading(false);
    }
  }, [workspace, profile.skills]);

  useEffect(() => {
    if (activeTab === 'skills') {
      const timer = window.setTimeout(loadSkills, 0);
      return () => window.clearTimeout(timer);
    }
  }, [activeTab, loadSkills]);

  const handleSaveSkills = async () => {
    setSkillsSaving(true);
    const nextProfile = { ...profile, skills: Array.from(selectedSkills) };
    const res = await saveProfile(nextProfile, agentBody);
    setSkillsSaving(false);
    if (res.success) {
      setSkillsChanged(false);
    }
  };

  const loadTools = useCallback(async () => {
    setToolsLoading(true);
    try {
      const toolsRes = await api.listTools();
      if (toolsRes.success && toolsRes.data) {
        setAllTools(toolsRes.data);
      } else {
        setAllTools([]);
      }
      setSelectedTools(new Set(profile.tools || []));
      setToolsChanged(false);
    } finally {
      setToolsLoading(false);
    }
  }, [profile.tools]);

  useEffect(() => {
    if (activeTab === 'tools') {
      const timer = window.setTimeout(loadTools, 0);
      return () => window.clearTimeout(timer);
    }
  }, [activeTab, loadTools]);

  const handleSaveTools = async () => {
    setToolsSaving(true);
    const nextProfile = { ...profile, tools: Array.from(selectedTools) };
    const res = await saveProfile(nextProfile, agentBody);
    setToolsSaving(false);
    if (res.success) {
      setToolsChanged(false);
    }
  };

  const updateProfile = (updates: Partial<AgentProfile>) => {
    setProfile((prev) => ({ ...prev, ...updates, name: agentName }));
    setProfileChanged(true);
  };

  const handleSaveConfig = async () => {
    setProfileSaving(true);
    const nextProfile = {
      ...profile,
      name: agentName,
      project_agents: linesToList(projectAgentsDraft),
    };
    const res = await saveProfile(nextProfile, agentBody);
    setProfileSaving(false);
    if (res.success) {
      setProfileChanged(false);
    }
  };

  const tabs: { key: NavTab; label: string }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'files', label: '文件' },
    { key: 'skills', label: '技能' },
    { key: 'tools', label: '工具' },
    { key: 'config', label: '配置' },
  ];

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
              <Bot className="w-4 h-4 text-accent" />
            </div>
            <div>
              <h2 className="text-base font-semibold text-text-primary">{agentName}</h2>
              <p className="text-xs text-text-muted font-mono">
                system/agents/{agentName}/AGENT.md
              </p>
            </div>
          </div>
        </div>
        {hasChanges && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-text-muted">有未保存的更改</span>
            <button
              onClick={handleSave}
              disabled={saving}
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
        )}
      </div>

      <div className="flex border-b border-border px-6 shrink-0">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-4 py-2.5 text-sm font-medium transition-colors border-b-2 ${
              activeTab === tab.key
                ? 'text-accent border-accent'
                : 'text-text-muted border-transparent hover:text-text-secondary'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 flex overflow-hidden">
        {activeTab === 'files' && (
          <>
            <div className="w-52 border-r border-border bg-bg-secondary overflow-y-auto py-2">
              <div className="px-3 py-1.5 text-[11px] font-medium text-text-muted uppercase tracking-wider">
                文件
              </div>
              {files.map((file) => (
                <button
                  key={file}
                  onClick={() => setSelectedFile(file)}
                  className={`flex items-center gap-2 w-full px-3 py-2 text-xs transition-colors ${
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
                <p className="px-3 py-2 text-xs text-text-muted italic">暂无文件</p>
              )}
            </div>

            <div className="flex-1 flex flex-col min-w-0">
              {selectedFile ? (
                <>
                  <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-bg-secondary shrink-0">
                    <span className="text-xs font-medium text-text-secondary">
                      {selectedFile}
                    </span>
                    {hasChanges && (
                      <span className="text-[10px] text-accent bg-accent/10 px-1.5 py-0.5 rounded">
                        已修改
                      </span>
                    )}
                  </div>
                  <div className="flex-1 overflow-y-auto">
                    {loading ? (
                      <div className="flex items-center justify-center h-full text-text-muted">
                        <Loader2 className="w-4 h-4 animate-spin mr-2" />
                        <span className="text-sm">加载中...</span>
                      </div>
                    ) : (
                      <textarea
                        value={editedContent}
                        onChange={(event) => handleContentChange(event.target.value)}
                        className="w-full h-full min-h-[400px] p-4 bg-bg-primary text-sm font-mono text-text-primary resize-none outline-none leading-relaxed"
                        spellCheck={false}
                      />
                    )}
                  </div>

                  <div className="flex items-center justify-end gap-3 px-4 py-3 border-t border-border bg-bg-secondary shrink-0">
                    <button
                      onClick={() => {
                        setEditedContent(fileContent);
                        setHasChanges(false);
                        setSaveStatus('idle');
                      }}
                      disabled={!hasChanges}
                      className="px-4 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      取消
                    </button>
                    <button
                      onClick={handleSave}
                      disabled={saving || !hasChanges}
                      className="flex items-center gap-1.5 px-4 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      {saving ? (
                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                      ) : saveStatus === 'success' ? (
                        <Check className="w-3.5 h-3.5" />
                      ) : (
                        <Save className="w-3.5 h-3.5" />
                      )}
                      <span>{saving ? '保存中' : saveStatus === 'success' ? '已保存' : '保存'}</span>
                    </button>
                  </div>
                </>
              ) : (
                <div className="flex-1 flex items-center justify-center text-text-muted">
                  <p className="text-sm">选择一个文件进行编辑</p>
                </div>
              )}
            </div>
          </>
        )}

        {activeTab === 'skills' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-4xl mx-auto">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      const next = new Set(allSkills.map((skill) => skill.name));
                      setSelectedSkills(next);
                      setSkillsChanged(true);
                    }}
                    disabled={allSkills.length === 0}
                    className="px-3 py-1.5 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40"
                  >
                    全选
                  </button>
                  <button
                    onClick={() => {
                      setSelectedSkills(new Set());
                      setSkillsChanged(true);
                    }}
                    disabled={selectedSkills.size === 0}
                    className="px-3 py-1.5 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40"
                  >
                    清空
                  </button>
                </div>
                <span className="text-sm text-text-muted">
                  {selectedSkills.size} / {allSkills.length} 已选
                </span>
              </div>

              {skillsLoading ? (
                <div className="flex items-center justify-center py-12 text-text-muted">
                  <Loader2 className="w-5 h-5 animate-spin mr-2" />
                  <span className="text-sm">加载中...</span>
                </div>
              ) : allSkills.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 text-text-muted">
                  <Wrench className="w-8 h-8 mb-3 opacity-40" />
                  <p className="text-sm">暂无可用技能</p>
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {allSkills.map((skill) => {
                    const isSelected = selectedSkills.has(skill.name);
                    return (
                      <button
                        key={skill.name}
                        onClick={() => {
                          const next = new Set(selectedSkills);
                          if (isSelected) {
                            next.delete(skill.name);
                          } else {
                            next.add(skill.name);
                          }
                          setSelectedSkills(next);
                          setSkillsChanged(true);
                        }}
                        className={`flex items-center justify-between px-4 py-3 rounded-card border text-left transition-all ${
                          isSelected
                            ? 'border-accent bg-accent/5'
                            : 'border-border bg-bg-secondary hover:border-border-hover'
                        }`}
                      >
                        <span
                          className={`text-sm font-medium truncate ${
                            isSelected ? 'text-accent' : 'text-text-primary'
                          }`}
                        >
                          {skill.name}
                        </span>
                        <div
                          className={`relative w-10 h-5 rounded-full transition-colors shrink-0 ml-3 ${
                            isSelected ? 'bg-accent' : 'bg-border'
                          }`}
                        >
                          <div
                            className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-transform ${
                              isSelected ? 'translate-x-5' : 'translate-x-0.5'
                            }`}
                          />
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}

              <div className="flex items-center justify-end gap-3 mt-8 pt-4 border-t border-border">
                <button
                  onClick={() => {
                    setSelectedSkills(new Set(profile.skills || []));
                    setSkillsChanged(false);
                  }}
                  disabled={!skillsChanged || skillsSaving}
                  className="px-4 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  取消
                </button>
                <button
                  onClick={handleSaveSkills}
                  disabled={!skillsChanged || skillsSaving}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {skillsSaving ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Save className="w-3.5 h-3.5" />
                  )}
                  <span>{skillsSaving ? '保存中' : '保存'}</span>
                </button>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'tools' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-4xl mx-auto">
              <div className="flex items-center justify-between mb-6">
                <p className="text-sm text-text-muted">
                  切换内置工具的启用状态，保存后写入 AGENT.md 的 tools frontmatter。
                </p>
                <span className="text-sm px-2.5 py-1 rounded-full border border-border text-text-muted bg-bg-secondary">
                  {selectedTools.size}/{allTools.length} 已启用
                </span>
              </div>

              {toolsLoading ? (
                <div className="flex items-center justify-center py-12 text-text-muted">
                  <Loader2 className="w-5 h-5 animate-spin mr-2" />
                  <span className="text-sm">加载中...</span>
                </div>
              ) : allTools.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 text-text-muted">
                  <Hammer className="w-8 h-8 mb-3 opacity-40" />
                  <p className="text-sm">暂无可用工具</p>
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {allTools.map((tool) => {
                    const isSelected = selectedTools.has(tool.name);
                    return (
                      <button
                        key={tool.name}
                        onClick={() => {
                          const next = new Set(selectedTools);
                          if (isSelected) {
                            next.delete(tool.name);
                          } else {
                            next.add(tool.name);
                          }
                          setSelectedTools(next);
                          setToolsChanged(true);
                        }}
                        className={`flex items-center justify-between px-4 py-3 rounded-card border text-left transition-all ${
                          isSelected
                            ? 'border-accent bg-accent/5'
                            : 'border-border bg-bg-secondary hover:border-border-hover'
                        }`}
                      >
                        <span
                          className={`text-sm font-medium truncate ${
                            isSelected ? 'text-accent' : 'text-text-primary'
                          }`}
                        >
                          {tool.name}
                        </span>
                        <div
                          className={`relative w-10 h-5 rounded-full transition-colors shrink-0 ml-3 ${
                            isSelected ? 'bg-accent' : 'bg-border'
                          }`}
                        >
                          <div
                            className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-transform ${
                              isSelected ? 'translate-x-5' : 'translate-x-0.5'
                            }`}
                          />
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}

              <div className="flex items-center justify-end gap-3 mt-8 pt-4 border-t border-border">
                <button
                  onClick={() => {
                    setSelectedTools(new Set(profile.tools || []));
                    setToolsChanged(false);
                  }}
                  disabled={!toolsChanged || toolsSaving}
                  className="px-4 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  取消
                </button>
                <button
                  onClick={handleSaveTools}
                  disabled={!toolsChanged || toolsSaving}
                  className="flex items-center gap-1.5 px-4 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {toolsSaving ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Save className="w-3.5 h-3.5" />
                  )}
                  <span>{toolsSaving ? '保存中' : '保存'}</span>
                </button>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'config' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-3xl mx-auto">
              {profileLoading ? (
                <div className="flex items-center justify-center py-12 text-text-muted">
                  <Loader2 className="w-5 h-5 animate-spin mr-2" />
                  <span className="text-sm">加载中...</span>
                </div>
              ) : (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="bg-bg-secondary border border-border rounded-card p-4">
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        名称
                      </label>
                      <input
                        type="text"
                        value={agentName}
                        disabled
                        className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-muted outline-none"
                      />
                    </div>

                    <div className="bg-bg-secondary border border-border rounded-card p-4">
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        描述
                      </label>
                      <input
                        type="text"
                        value={profile.description}
                        onChange={(event) => updateProfile({ description: event.target.value })}
                        placeholder="例如：日常对话分析"
                        className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary outline-none focus:border-accent transition-colors"
                      />
                    </div>

                    <div className="bg-bg-secondary border border-border rounded-card p-4">
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        开发态模型
                      </label>
                      <input
                        type="text"
                        value={profile.model}
                        onChange={(event) => updateProfile({ model: event.target.value })}
                        placeholder="例如：minimax-m2.7"
                        className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary outline-none focus:border-accent transition-colors"
                      />
                    </div>

                    <div className="bg-bg-secondary border border-border rounded-card p-4">
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        最大轮数
                      </label>
                      <input
                        type="number"
                        min={1}
                        value={profile.maxTurns}
                        onChange={(event) => updateProfile({ maxTurns: Number(event.target.value) || 1 })}
                        className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary outline-none focus:border-accent transition-colors"
                      />
                    </div>

                    <div className="bg-bg-secondary border border-border rounded-card p-4">
                      <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                        Memory
                      </label>
                      <input
                        type="text"
                        value={profile.memory}
                        onChange={(event) => updateProfile({ memory: event.target.value })}
                        placeholder="例如：project"
                        className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary outline-none focus:border-accent transition-colors"
                      />
                    </div>
                  </div>

                  <div className="mt-4 bg-bg-secondary border border-border rounded-card p-4">
                    <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                      Project Agents
                    </label>
                    <textarea
                      value={projectAgentsDraft}
                      onChange={(event) => {
                        setProjectAgentsDraft(event.target.value);
                        setProfileChanged(true);
                      }}
                      placeholder="每行一个 agent 名称"
                      rows={3}
                      className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary resize-none outline-none focus:border-accent transition-colors leading-relaxed"
                      spellCheck={false}
                    />
                  </div>

                  <div className="mt-4 bg-bg-secondary border border-border rounded-card p-4">
                    <label className="block text-xs font-medium text-text-muted uppercase tracking-wider mb-2">
                      AGENT.md 正文
                    </label>
                    <textarea
                      value={agentBody}
                      onChange={(event) => {
                        setAgentBody(event.target.value);
                        setProfileChanged(true);
                      }}
                      placeholder="输入 agent 角色、职责、执行规范..."
                      rows={10}
                      className="w-full px-3 py-2 rounded-lg border border-border bg-bg-primary text-sm text-text-primary font-mono resize-y outline-none focus:border-accent transition-colors leading-relaxed"
                      spellCheck={false}
                    />
                  </div>

                  <div className="flex items-center justify-end gap-3 mt-8 pt-4 border-t border-border">
                    <button
                      onClick={() => {
                        loadAgentProfile();
                        setProfileChanged(false);
                      }}
                      disabled={!profileChanged || profileSaving}
                      className="px-4 py-2 rounded-button border border-border text-sm text-text-secondary hover:bg-bg-tertiary transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      取消
                    </button>
                    <button
                      onClick={handleSaveConfig}
                      disabled={!profileChanged || profileSaving}
                      className="flex items-center gap-1.5 px-4 py-2 rounded-button bg-accent hover:bg-accent-hover text-white text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                    >
                      {profileSaving ? (
                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                      ) : (
                        <Save className="w-3.5 h-3.5" />
                      )}
                      <span>{profileSaving ? '保存中' : '保存'}</span>
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        )}

        {activeTab === 'overview' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-3xl mx-auto space-y-5">
              {/* Hero Card */}
              <div className="relative overflow-hidden bg-bg-secondary border border-border rounded-card p-6">
                <div className="absolute top-0 right-0 w-32 h-32 bg-accent/5 rounded-full -translate-y-1/2 translate-x-1/2" />
                <div className="relative flex items-center gap-4">
                  <div className="w-16 h-16 rounded-2xl bg-accent/10 flex items-center justify-center shrink-0 ring-2 ring-accent/20">
                    <span className="text-2xl font-bold text-accent">
                      {agentName.charAt(0).toUpperCase()}
                    </span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <h2 className="text-2xl font-bold text-text-primary">{agentName}</h2>
                    <p className="text-sm text-text-secondary mt-1.5 leading-relaxed">
                      {profile.description || '未配置描述'}
                    </p>
                  </div>
                </div>
              </div>

              {/* Stats Row */}
              <div className="grid grid-cols-3 gap-3">
                <div className="bg-bg-secondary border border-border rounded-card p-4 text-center hover:border-accent/20 transition-colors">
                  <div className="flex items-center justify-center gap-1.5 mb-2">
                    <Layers className="w-4 h-4 text-accent" />
                    <span className="text-xs font-medium text-text-muted">文件</span>
                  </div>
                  <p className="text-2xl font-bold text-text-primary">{files.length}</p>
                </div>
                <div className="bg-bg-secondary border border-border rounded-card p-4 text-center hover:border-accent/20 transition-colors">
                  <div className="flex items-center justify-center gap-1.5 mb-2">
                    <Zap className="w-4 h-4 text-accent" />
                    <span className="text-xs font-medium text-text-muted">技能</span>
                  </div>
                  <p className="text-2xl font-bold text-text-primary">{selectedSkills.size}</p>
                </div>
                <div className="bg-bg-secondary border border-border rounded-card p-4 text-center hover:border-accent/20 transition-colors">
                  <div className="flex items-center justify-center gap-1.5 mb-2">
                    <Cpu className="w-4 h-4 text-accent" />
                    <span className="text-xs font-medium text-text-muted">工具</span>
                  </div>
                  <p className="text-2xl font-bold text-text-primary">{selectedTools.size}</p>
                </div>
              </div>

              {/* Progress Bars */}
              <div className="bg-bg-secondary border border-border rounded-card p-5 space-y-4">
                <div className="flex items-center gap-2 mb-1">
                  <BarChart3 className="w-4 h-4 text-accent" />
                  <span className="text-sm font-semibold text-text-primary">配置概览</span>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-xs text-text-muted">模型</span>
                    <span className="text-xs font-medium text-text-primary">{profile.model || '默认'}</span>
                  </div>
                  <div className="h-2 bg-bg-tertiary rounded-full overflow-hidden">
                    <div className="h-full bg-accent/60 rounded-full" style={{ width: profile.model ? '100%' : '0%' }} />
                  </div>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-xs text-text-muted">最大轮数</span>
                    <span className="text-xs font-medium text-text-primary">{profile.maxTurns || 300}</span>
                  </div>
                  <div className="h-2 bg-bg-tertiary rounded-full overflow-hidden">
                    <div
                      className="h-full bg-accent/60 rounded-full transition-all"
                      style={{ width: `${Math.min(((profile.maxTurns || 300) / 500) * 100, 100)}%` }}
                    />
                  </div>
                </div>

                <div>
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-xs text-text-muted">描述</span>
                    <span className="text-xs font-medium text-text-primary">{profile.description ? '已配置' : '未配置'}</span>
                  </div>
                  <div className="h-2 bg-bg-tertiary rounded-full overflow-hidden">
                    <div className="h-full bg-accent/60 rounded-full" style={{ width: profile.description ? '100%' : '0%' }} />
                  </div>
                </div>
              </div>

              {/* Info Grid */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div className="bg-bg-secondary border border-border rounded-card p-4 hover:border-accent/20 transition-colors">
                  <div className="flex items-center gap-2 mb-2">
                    <FolderOpen className="w-4 h-4 text-accent" />
                    <span className="text-xs font-medium text-text-muted">路径</span>
                  </div>
                  <p className="text-sm text-text-primary font-mono leading-relaxed">
                    workspace/{workspace}/system/agents/{agentName}
                  </p>
                </div>

                <div className="bg-bg-secondary border border-border rounded-card p-4 hover:border-accent/20 transition-colors">
                  <div className="flex items-center gap-2 mb-2">
                    <FileCode className="w-4 h-4 text-accent" />
                    <span className="text-xs font-medium text-text-muted">配置来源</span>
                  </div>
                  <p className="text-sm text-text-primary font-mono">AGENT.md frontmatter</p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
