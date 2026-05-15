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
} from 'lucide-react';
import { api } from '../api/client';
import type { WorkspaceSkill, WorkspaceTool } from '../types';

interface AgentDetailProps {
  workspace: string;
  agentName: string;
  onBack: () => void;
}

type NavTab = 'overview' | 'files' | 'skills' | 'tools';

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

  // Skills state
  const [allSkills, setAllSkills] = useState<WorkspaceSkill[]>([]);
  const [selectedSkills, setSelectedSkills] = useState<Set<string>>(new Set());
  const [skillsLoading, setSkillsLoading] = useState(false);
  const [skillsSaving, setSkillsSaving] = useState(false);
  const [skillsChanged, setSkillsChanged] = useState(false);

  // Tools state
  const [allTools, setAllTools] = useState<WorkspaceTool[]>([]);
  const [selectedTools, setSelectedTools] = useState<Set<string>>(new Set());
  const [toolsLoading, setToolsLoading] = useState(false);
  const [toolsSaving, setToolsSaving] = useState(false);
  const [toolsChanged, setToolsChanged] = useState(false);

  const loadFiles = useCallback(() => {
    api.listAgents(workspace).then((res) => {
      if (res.success && res.data) {
        const agent = res.data.find((a) => a.name === agentName);
        if (agent) {
          setFiles(agent.files);
          if (agent.files.length > 0 && !selectedFile) {
            setSelectedFile(agent.files[0]);
          }
        }
      }
    });
  }, [workspace, agentName, selectedFile]);

  useEffect(() => {
    loadFiles();
  }, [loadFiles]);

  useEffect(() => {
    if (!selectedFile) return;
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
  }, [workspace, agentName, selectedFile]);

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
      setTimeout(() => setSaveStatus('idle'), 2000);
    } else {
      setSaveStatus('error');
    }
  };

  const handleContentChange = (value: string) => {
    setEditedContent(value);
    setHasChanges(value !== fileContent);
  };

  // Load skills list and agent's skill config
  const loadSkillsConfig = useCallback(async () => {
    setSkillsLoading(true);
    try {
      // Load all available skills
      const skillsRes = await api.listSkills(workspace);
      if (skillsRes.success && skillsRes.data) {
        setAllSkills(skillsRes.data);
      }

      // Load agent's current skill config
      const configPath = `system/agents/${agentName}/skills.json`;
      const configRes = await api.readWorkspaceFile(workspace, configPath);
      if (configRes.success && configRes.data) {
        try {
          const config = JSON.parse(configRes.data.content);
          if (Array.isArray(config.skills)) {
            setSelectedSkills(new Set(config.skills));
          }
        } catch {
          // Invalid JSON, ignore
        }
      } else {
        setSelectedSkills(new Set());
      }
      setSkillsChanged(false);
    } finally {
      setSkillsLoading(false);
    }
  }, [workspace, agentName]);

  useEffect(() => {
    if (activeTab === 'skills') {
      loadSkillsConfig();
    }
  }, [activeTab, loadSkillsConfig]);

  const handleSaveSkills = async () => {
    setSkillsSaving(true);
    const configPath = `system/agents/${agentName}/skills.json`;
    const config = { skills: Array.from(selectedSkills) };
    const res = await api.writeWorkspaceFile(
      workspace,
      configPath,
      JSON.stringify(config, null, 2)
    );
    setSkillsSaving(false);
    if (res.success) {
      setSkillsChanged(false);
    }
  };

  // Load tools list and agent's tool config
  const loadToolsConfig = useCallback(async () => {
    setToolsLoading(true);
    try {
      // Load all available tools
      const toolsRes = await api.listTools();
      if (toolsRes.success && toolsRes.data) {
        setAllTools(toolsRes.data);
      }

      // Load agent's current tool config
      const configPath = `system/agents/${agentName}/tools.json`;
      const configRes = await api.readWorkspaceFile(workspace, configPath);
      if (configRes.success && configRes.data) {
        try {
          const config = JSON.parse(configRes.data.content);
          if (Array.isArray(config.tools)) {
            setSelectedTools(new Set(config.tools));
          }
        } catch {
          // Invalid JSON, ignore
        }
      } else {
        // Default: all tools enabled
        setSelectedTools(new Set(toolsRes.data?.map((t) => t.name) || []));
      }
      setToolsChanged(false);
    } finally {
      setToolsLoading(false);
    }
  }, [workspace, agentName]);

  useEffect(() => {
    if (activeTab === 'tools') {
      loadToolsConfig();
    }
  }, [activeTab, loadToolsConfig]);

  const handleSaveTools = async () => {
    setToolsSaving(true);
    const configPath = `system/agents/${agentName}/tools.json`;
    const config = { tools: Array.from(selectedTools) };
    const res = await api.writeWorkspaceFile(
      workspace,
      configPath,
      JSON.stringify(config, null, 2)
    );
    setToolsSaving(false);
    if (res.success) {
      setToolsChanged(false);
    }
  };

  const tabs: { key: NavTab; label: string }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'files', label: '文件' },
    { key: 'skills', label: '技能' },
    { key: 'tools', label: '工具' },
  ];

  return (
    <div className="flex flex-col flex-1 h-full overflow-hidden bg-bg-primary">
      {/* Header */}
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
                system/agents/{agentName}
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

      {/* Nav Tabs */}
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

      {/* Content */}
      <div className="flex-1 flex overflow-hidden">
        {activeTab === 'files' && (
          <>
            {/* File Tree */}
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

            {/* Editor */}
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
                        onChange={(e) => handleContentChange(e.target.value)}
                        className="w-full h-full min-h-[400px] p-4 bg-bg-primary text-sm font-mono text-text-primary resize-none outline-none leading-relaxed"
                        spellCheck={false}
                      />
                    )}
                  </div>

                  {/* Bottom Action Bar - always show */}
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
              {/* Toolbar */}
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      const newSet = new Set(allSkills.map((s) => s.name));
                      setSelectedSkills(newSet);
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

              {/* Skills Grid */}
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
                          const newSet = new Set(selectedSkills);
                          if (isSelected) {
                            newSet.delete(skill.name);
                          } else {
                            newSet.add(skill.name);
                          }
                          setSelectedSkills(newSet);
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

              {/* Bottom Action Bar */}
              <div className="flex items-center justify-between mt-8 pt-4 border-t border-border">
                <button
                  onClick={async () => {
                    if (!confirm('确定要删除此 Agent 吗？此操作不可撤销。')) return;
                    // TODO: implement agent deletion
                  }}
                  className="px-4 py-2 rounded-button border border-red-200 text-sm text-red-500 hover:bg-red-50 transition-colors"
                >
                  删除
                </button>
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      // Reset to saved state
                      loadSkillsConfig();
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
          </div>
        )}

        {activeTab === 'tools' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-4xl mx-auto">
              {/* Description + Status */}
              <div className="flex items-center justify-between mb-6">
                <p className="text-sm text-text-muted">
                  切换内置工具的启用状态。启用后 Agent 可在对话中调用对应工具。
                </p>
                <span
                  className={`text-sm px-2.5 py-1 rounded-full border ${
                    selectedTools.size === allTools.length
                      ? 'border-green-200 text-green-600 bg-green-50'
                      : 'border-border text-text-muted bg-bg-secondary'
                  }`}
                >
                  {selectedTools.size}/{allTools.length} 已启用
                </span>
              </div>

              {/* Tools Grid */}
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
                          const newSet = new Set(selectedTools);
                          if (isSelected) {
                            newSet.delete(tool.name);
                          } else {
                            newSet.add(tool.name);
                          }
                          setSelectedTools(newSet);
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

              {/* Bottom Action Bar */}
              <div className="flex items-center justify-between mt-8 pt-4 border-t border-border">
                <button
                  onClick={async () => {
                    if (!confirm('确定要删除此 Agent 吗？此操作不可撤销。')) return;
                    // TODO: implement agent deletion
                  }}
                  className="px-4 py-2 rounded-button border border-red-200 text-sm text-red-500 hover:bg-red-50 transition-colors"
                >
                  删除
                </button>
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      loadToolsConfig();
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
          </div>
        )}

        {activeTab === 'overview' && (
          <div className="flex-1 overflow-y-auto px-6 py-6">
            <div className="max-w-2xl">
              <h3 className="text-lg font-semibold text-text-primary mb-4">Agent 信息</h3>
              <div className="space-y-4">
                <div className="bg-bg-secondary border border-border rounded-card p-4">
                  <label className="text-xs font-medium text-text-muted uppercase tracking-wider">
                    名称
                  </label>
                  <p className="text-sm text-text-primary mt-1 font-mono">{agentName}</p>
                </div>
                <div className="bg-bg-secondary border border-border rounded-card p-4">
                  <label className="text-xs font-medium text-text-muted uppercase tracking-wider">
                    路径
                  </label>
                  <p className="text-sm text-text-primary mt-1 font-mono">
                    workspace/{workspace}/system/agents/{agentName}
                  </p>
                </div>
                <div className="bg-bg-secondary border border-border rounded-card p-4">
                  <label className="text-xs font-medium text-text-muted uppercase tracking-wider">
                    文件数
                  </label>
                  <p className="text-sm text-text-primary mt-1">{files.length}</p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
