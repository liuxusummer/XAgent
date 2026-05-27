import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Bot,
  ChevronDown,
  Clock,
  Pencil,
  Play,
  Plus,
  Sparkles,
  Trash2,
  X,
} from 'lucide-react';
import { api } from '../api/client';
import type { ScheduledTask, ScheduledTaskWriteRequest } from '../types';

type TabView = 'today' | 'all';

interface CronPanelProps {
  workspace: string;
  configPath?: string;
  observabilityConfigPath?: string;
  onDebugRunStarted?: (task: ScheduledTask, sessionId: string) => void;
}

interface NewTaskModalProps {
  open: boolean;
  agents: string[];
  editJob?: ScheduledTask;
  error: string;
  onClose: () => void;
  onSave: (payload: Omit<ScheduledTaskWriteRequest, 'ws' | 'config_path' | 'observability_config_path'>) => void;
}

function todayKey() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${now.getFullYear()}-${month}-${day}`;
}

function formatShortDate(value: string | null) {
  if (!value) return '--';
  return value.slice(5);
}

function repeatLabel(job: ScheduledTask) {
  if (job.repeat === 'daily') return job.time ? `Daily at ${job.time}` : 'Daily';
  if (job.repeat === 'weekly') return job.time ? `Weekly at ${job.time}` : 'Weekly';
  if (job.repeat === 'custom') return `Every ${job.interval_minutes || 60} min`;
  return 'No repeat';
}

function agentLabel(agent: string) {
  return agent || 'default';
}

function initialTaskForm(editJob?: ScheduledTask) {
  return {
    name: editJob?.name || '',
    repeat: editJob?.repeat || 'none',
    date: editJob?.date || '',
    time: editJob?.time || '',
    endDate: editJob?.end_date || '',
    intervalMinutes: editJob?.interval_minutes || 60,
    prompt: editJob?.prompt || '',
    keepOneChat: Boolean(editJob?.keep_one_chat),
    selectedAgent: editJob ? agentLabel(editJob.agent) : 'default',
  };
}

function NewTaskModal({ open, agents, editJob, error, onClose, onSave }: NewTaskModalProps) {
  const initialForm = initialTaskForm(editJob);
  const [name, setName] = useState(initialForm.name);
  const [repeat, setRepeat] = useState<ScheduledTask['repeat']>(initialForm.repeat as ScheduledTask['repeat']);
  const [date, setDate] = useState(initialForm.date);
  const [time, setTime] = useState(initialForm.time);
  const [endDate, setEndDate] = useState(initialForm.endDate);
  const [intervalMinutes, setIntervalMinutes] = useState(initialForm.intervalMinutes);
  const [prompt, setPrompt] = useState(initialForm.prompt);
  const [keepOneChat, setKeepOneChat] = useState(initialForm.keepOneChat);
  const [selectedAgent, setSelectedAgent] = useState(initialForm.selectedAgent);
  const [showAgentDropdown, setShowAgentDropdown] = useState(false);

  if (!open) return null;

  const save = () => {
    onSave({
      name,
      prompt,
      agent: selectedAgent === 'default' ? '' : selectedAgent,
      repeat,
      date,
      time,
      end_date: endDate,
      interval_minutes: repeat === 'custom' ? intervalMinutes : 0,
      keep_one_chat: keepOneChat,
      status: 'running',
    });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/30 px-4 pb-8 pt-16">
      <div className="w-full max-w-2xl rounded-card border border-border bg-bg-primary shadow-lg">
        <div className="flex items-center justify-between border-b border-border px-6 py-4">
          <h3 className="text-base font-semibold text-text-primary">{editJob ? 'Edit Task' : 'New Task'}</h3>
          <button onClick={onClose} className="rounded-button p-1.5 text-text-muted transition-colors hover:bg-bg-tertiary hover:text-text-secondary">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="space-y-5 px-6 py-5">
          {error && (
            <div className="rounded-button border border-status-error/30 bg-status-error/10 px-3 py-2 text-xs text-status-error">
              {error}
            </div>
          )}

          <div>
            <label className="mb-1.5 block text-sm font-medium text-text-primary">
              Name <span className="text-status-error">*</span>
            </label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="w-full rounded-button border border-border bg-bg-secondary px-3.5 py-2 text-sm text-text-primary outline-none placeholder:text-text-muted focus:border-accent/50"
              placeholder="Summary of AI News"
            />
          </div>

          <div>
            <label className="mb-1.5 block text-sm font-medium text-text-primary">
              Repeat <span className="text-status-error">*</span>
            </label>
            <div className="flex gap-3">
              <select
                value={repeat}
                onChange={(e) => setRepeat(e.target.value as ScheduledTask['repeat'])}
                className="min-w-[160px] cursor-pointer appearance-none rounded-button border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
              >
                <option value="none">No Repeat</option>
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
                <option value="custom">Custom</option>
              </select>
              <input
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className="flex-1 rounded-button border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
              />
              <input
                type="time"
                value={time}
                onChange={(e) => setTime(e.target.value)}
                className="w-[150px] rounded-button border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
              />
            </div>
          </div>

          <div>
            <label className="mb-1.5 block text-sm font-medium text-text-primary">Ends</label>
            <div className="flex items-center gap-3">
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="w-[200px] rounded-button border border-border bg-bg-secondary px-3 py-2 text-sm text-text-primary outline-none focus:border-accent/50"
              />
              {repeat === 'custom' && (
                <label className="flex items-center gap-2 text-xs text-text-secondary">
                  Every
                  <input
                    type="number"
                    min={1}
                    value={intervalMinutes}
                    onChange={(e) => setIntervalMinutes(Math.max(1, Number(e.target.value) || 1))}
                    className="w-20 rounded-button border border-border bg-bg-secondary px-2 py-1.5 text-sm text-text-primary outline-none focus:border-accent/50"
                  />
                  min
                </label>
              )}
            </div>
          </div>

          <div>
            <div className="mb-1.5 flex items-center justify-between">
              <label className="text-sm font-medium text-text-primary">
                Prompt <span className="text-status-error">*</span>
              </label>
              <div className="flex items-center gap-3">
                <div className="relative">
                  <button
                    onClick={() => setShowAgentDropdown(!showAgentDropdown)}
                    onBlur={() => setTimeout(() => setShowAgentDropdown(false), 150)}
                    className={`flex items-center gap-1.5 rounded-button px-2 py-1 text-xs font-medium transition-colors ${
                      selectedAgent !== 'default'
                        ? 'bg-accent/10 text-accent'
                        : 'text-text-secondary hover:bg-accent/5 hover:text-accent'
                    }`}
                  >
                    <Bot className="h-3.5 w-3.5" />
                    Agent
                    {selectedAgent !== 'default' && <span className="ml-0.5 text-[10px] opacity-70">({selectedAgent})</span>}
                    <ChevronDown className={`h-3 w-3 transition-transform ${showAgentDropdown ? 'rotate-180' : ''}`} />
                  </button>
                  {showAgentDropdown && (
                    <div className="absolute right-0 top-full z-10 mt-1 w-44 rounded-card border border-border bg-bg-primary py-1 shadow-lg">
                      {agents.map((agent) => (
                        <button
                          key={agent}
                          onMouseDown={() => {
                            setSelectedAgent(agent);
                            setShowAgentDropdown(false);
                          }}
                          className={`flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors ${
                            selectedAgent === agent
                              ? 'bg-accent/10 font-medium text-accent'
                              : 'text-text-secondary hover:bg-bg-tertiary hover:text-text-primary'
                          }`}
                        >
                          {selectedAgent === agent && <span className="h-1.5 w-1.5 rounded-full bg-accent" />}
                          {agent}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <button className="flex items-center gap-1 text-xs text-text-secondary transition-colors hover:text-accent">
                  <Sparkles className="h-3.5 w-3.5" />
                  Template
                </button>
              </div>
            </div>
            <textarea
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              rows={5}
              className="w-full resize-none rounded-card border border-border bg-bg-secondary px-3.5 py-2.5 text-sm leading-relaxed text-text-primary outline-none placeholder:text-text-muted focus:border-accent/50"
              placeholder='Ask anything, use "/" to select a skill or "@" to reference a resource'
            />
          </div>

          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-medium text-text-primary">Keep in One Chat</div>
              <p className="mt-0.5 text-xs leading-relaxed text-text-muted">
                All repeated task results will appear in a single chat, making it easy to review and compare across runs.
              </p>
            </div>
            <button
              onClick={() => setKeepOneChat(!keepOneChat)}
              className={`relative ml-4 inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${
                keepOneChat ? 'bg-accent' : 'bg-bg-tertiary'
              }`}
            >
              <span
                className={`inline-block h-3.5 w-3.5 rounded-full bg-white shadow-sm transition-transform ${
                  keepOneChat ? 'translate-x-5' : 'translate-x-0.5'
                }`}
              />
            </button>
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 border-t border-border px-6 py-4">
          <button onClick={onClose} className="rounded-button border border-border px-4 py-2 text-sm font-medium text-text-secondary transition-colors hover:bg-bg-tertiary">
            Cancel
          </button>
          <button onClick={save} className="rounded-button bg-text-primary px-4 py-2 text-sm font-medium text-white transition-opacity hover:opacity-90">
            Save
          </button>
        </div>
      </div>
    </div>
  );
}

export function CronPanel({
  workspace,
  configPath = '',
  observabilityConfigPath = '',
  onDebugRunStarted,
}: CronPanelProps) {
  const [jobs, setJobs] = useState<ScheduledTask[]>([]);
  const [agents, setAgents] = useState<string[]>(['default']);
  const [activeTab, setActiveTab] = useState<TabView>('today');
  const [showNewTask, setShowNewTask] = useState(false);
  const [editingJob, setEditingJob] = useState<ScheduledTask | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [modalError, setModalError] = useState('');

  const loadTasks = useCallback(async () => {
    setLoading(true);
    const result = await api.listScheduledTasks(workspace);
    if (result.success && result.data) {
      setJobs(result.data);
      setError('');
    } else {
      setError(result.error || 'Failed to load tasks');
    }
    setLoading(false);
  }, [workspace]);

  useEffect(() => {
    queueMicrotask(() => {
      loadTasks();
    });
    api.listAgents(workspace).then((result) => {
      if (result.success && result.data) {
        setAgents(['default', ...result.data.map((agent) => agent.name)]);
      }
    });
  }, [loadTasks, workspace]);

  const openNewTask = () => {
    setEditingJob(undefined);
    setModalError('');
    setShowNewTask(true);
  };

  const openEditTask = (job: ScheduledTask) => {
    setEditingJob(job);
    setModalError('');
    setShowNewTask(true);
  };

  const closeModal = () => {
    setShowNewTask(false);
    setEditingJob(undefined);
    setModalError('');
  };

  const saveJob = async (payload: Omit<ScheduledTaskWriteRequest, 'ws' | 'config_path' | 'observability_config_path'>) => {
    const request: ScheduledTaskWriteRequest = {
      ...payload,
      ws: workspace,
      config_path: configPath,
      observability_config_path: observabilityConfigPath,
    };
    const result = editingJob
      ? await api.updateScheduledTask(editingJob.id, request)
      : await api.createScheduledTask(request);
    if (!result.success) {
      setModalError(result.error || 'Failed to save task');
      return;
    }
    closeModal();
    loadTasks();
  };

  const toggleJobStatus = async (job: ScheduledTask) => {
    const nextStatus = job.status === 'running' ? 'paused' : 'running';
    const result = await api.setScheduledTaskStatus(workspace, job.id, nextStatus);
    if (!result.success) {
      setError(result.error || 'Failed to update task');
      return;
    }
    loadTasks();
  };

  const debugRunJob = async (job: ScheduledTask) => {
    const result = await api.debugRunScheduledTask(workspace, job.id);
    if (!result.success) {
      setError(result.error || 'Failed to debug run task');
      return;
    }
    const sessionId = result.data?.last_debug_session_id || '';
    if (result.data && sessionId) {
      onDebugRunStarted?.(result.data, sessionId);
    }
    loadTasks();
  };

  const deleteJob = async (job: ScheduledTask) => {
    const ok = window.confirm(`Delete task "${job.name}"?`);
    if (!ok) return;
    const result = await api.deleteScheduledTask(workspace, job.id);
    if (!result.success) {
      setError(result.error || 'Failed to delete task');
      return;
    }
    loadTasks();
  };

  const shownJobs = activeTab === 'today'
    ? jobs.filter((job) => job.next_run?.startsWith(todayKey()) || job.last_run?.startsWith(todayKey()))
    : jobs;
  const completedToday = useMemo(() => jobs.filter((job) => job.last_run?.startsWith(todayKey())).length, [jobs]);
  const pendingToday = useMemo(() => jobs.filter((job) => job.status === 'running' && job.next_run?.startsWith(todayKey())).length, [jobs]);
  const ongoingToday = useMemo(() => jobs.filter((job) => job.status === 'running').length, [jobs]);

  return (
    <div className="relative flex h-full flex-1 flex-col overflow-hidden bg-bg-primary">
      <div className="flex shrink-0 items-center justify-between px-6 pb-3 pt-5">
        <div className="flex items-center gap-3">
          <Clock className="h-5 w-5 text-accent" />
          <h2 className="text-lg font-bold text-text-primary">Tasks</h2>
          {loading && <span className="text-xs text-text-muted">Loading</span>}
        </div>
        <button
          onClick={openNewTask}
          className="flex items-center gap-1.5 rounded-button bg-accent px-3 py-1.5 text-xs font-medium text-white transition-colors hover:bg-accent-hover"
        >
          <Plus className="h-3.5 w-3.5" />
          <span>New Task</span>
        </button>
      </div>

      <div className="flex shrink-0 border-b border-border px-6">
        <button
          onClick={() => setActiveTab('today')}
          className={`border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
            activeTab === 'today'
              ? 'border-text-primary text-text-primary'
              : 'border-transparent text-text-muted hover:text-text-secondary'
          }`}
        >
          Today
        </button>
        <button
          onClick={() => setActiveTab('all')}
          className={`ml-6 border-b-2 px-3 py-2 text-sm font-medium transition-colors ${
            activeTab === 'all'
              ? 'border-text-primary text-text-primary'
              : 'border-transparent text-text-muted hover:text-text-secondary'
          }`}
        >
          All
        </button>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-5">
        {error && (
          <div className="mb-4 max-w-4xl rounded-button border border-status-error/30 bg-status-error/10 px-3 py-2 text-xs text-status-error">
            {error}
          </div>
        )}

        {activeTab === 'today' ? (
          <div className="max-w-4xl space-y-5">
            <div className="flex items-center gap-8 text-sm">
              <span className="flex items-center gap-2 text-text-secondary">
                <span className="h-2 w-2 rounded-full bg-status-success" />
                Completed {completedToday}
              </span>
              <span className="flex items-center gap-2 text-text-secondary">
                <span className="h-2 w-2 rounded-full bg-accent" />
                To be started {pendingToday}
              </span>
              <span className="flex items-center gap-2 text-text-secondary">
                <span className="h-2 w-2 rounded-full bg-status-warning" />
                On Going {ongoingToday}
              </span>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              {shownJobs.map((job) => (
                <div key={job.id} className="group flex items-start justify-between rounded-card border border-border bg-bg-secondary px-4 py-3.5 transition-all hover:border-border-hover">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="truncate text-sm font-medium text-text-primary">{job.name}</span>
                      {(job.last_error || job.last_debug_error) && <span className="rounded-button bg-status-error/10 px-1.5 py-0.5 text-[10px] text-status-error">Error</span>}
                    </div>
                    <div className="mt-1 text-[11px] leading-relaxed text-text-muted">
                      {job.last_run ? <>Completed at {job.last_run}</> : job.next_run ? <>Scheduled for {job.next_run}</> : <>Not yet scheduled</>}
                    </div>
                    {job.last_debug_run && (
                      <div className="mt-0.5 text-[11px] text-text-muted">Debugged at {job.last_debug_run}</div>
                    )}
                    <div className="mt-0.5 text-[11px] text-text-muted">{repeatLabel(job)}</div>
                  </div>
                  <div className="ml-3 flex shrink-0 items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100">
                    <button
                      onClick={() => debugRunJob(job)}
                      className="flex items-center gap-1 rounded-button px-2 py-1.5 text-[11px] font-medium text-text-muted transition-colors hover:bg-bg-tertiary hover:text-text-secondary"
                      aria-label="Debug run now"
                      title="Debug run now"
                    >
                      <Play className="h-3.5 w-3.5" />
                      <span>Debug</span>
                    </button>
                    <button onClick={() => openEditTask(job)} className="rounded-button p-1.5 text-text-muted transition-colors hover:bg-bg-tertiary hover:text-text-secondary" title="Edit">
                      <Pencil className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </div>
              ))}
            </div>

            {shownJobs.length === 0 && (
              <div className="flex flex-col items-center justify-center py-16">
                <Clock className="mb-3 h-8 w-8 text-text-muted opacity-40" />
                <p className="text-sm text-text-muted">No tasks scheduled for today</p>
              </div>
            )}
          </div>
        ) : (
          <div className="max-w-5xl">
            <div className="grid grid-cols-[1fr_2fr_120px_150px_80px_150px] gap-4 border-b border-border px-1 pb-2.5">
              <div className="text-xs font-medium text-text-muted">Task Name</div>
              <div className="text-xs font-medium text-text-muted">Prompt</div>
              <div className="text-xs font-medium text-text-muted">Next run</div>
              <div className="text-xs font-medium text-text-muted">Repeat at</div>
              <div className="text-xs font-medium text-text-muted">Status</div>
              <div className="text-xs font-medium text-text-muted">Action</div>
            </div>

            <div className="divide-y divide-border">
              {shownJobs.map((job) => (
                <div key={job.id} className="group grid grid-cols-[1fr_2fr_120px_150px_80px_150px] items-center gap-4 px-1 py-3.5 transition-colors hover:bg-bg-tertiary/50">
                  <div className="min-w-0">
                    <span className="block truncate text-sm font-medium text-text-primary">{job.name}</span>
                    <span className="block truncate text-[11px] text-text-muted">{agentLabel(job.agent)}</span>
                  </div>
                  <div className="min-w-0">
                    <span className="block truncate text-xs leading-relaxed text-text-secondary">{job.prompt}</span>
                  </div>
                  <div className="whitespace-nowrap text-xs text-text-primary">{formatShortDate(job.next_run)}</div>
                  <div className="whitespace-nowrap text-xs text-text-secondary">{repeatLabel(job)}</div>
                  <div>
                    <button
                      onClick={() => toggleJobStatus(job)}
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                        job.status === 'running' ? 'bg-accent' : 'bg-bg-tertiary'
                      }`}
                      title={job.status === 'running' ? 'Pause' : 'Resume'}
                    >
                      <span
                        className={`inline-block h-3.5 w-3.5 rounded-full bg-white shadow-sm transition-transform ${
                          job.status === 'running' ? 'translate-x-5' : 'translate-x-0.5'
                        }`}
                      />
                    </button>
                  </div>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => debugRunJob(job)}
                      className="flex items-center gap-1 rounded-button px-2 py-1 text-xs font-medium text-text-muted transition-colors hover:bg-bg-secondary hover:text-text-secondary"
                      aria-label="Debug run now"
                      title="Debug run now"
                    >
                      <Play className="h-4 w-4" />
                      <span>Debug</span>
                    </button>
                    <button onClick={() => openEditTask(job)} className="rounded-button p-1 text-text-muted transition-colors hover:bg-bg-secondary hover:text-text-secondary" title="Edit">
                      <Pencil className="h-4 w-4" />
                    </button>
                    <button onClick={() => deleteJob(job)} className="rounded-button p-1 text-text-muted transition-colors hover:bg-bg-secondary hover:text-status-error" title="Delete">
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>

            {shownJobs.length === 0 && (
              <div className="flex flex-col items-center justify-center py-16">
                <Clock className="mb-3 h-8 w-8 text-text-muted opacity-40" />
                <p className="text-sm text-text-muted">No cron jobs yet</p>
              </div>
            )}
          </div>
        )}
      </div>

      {showNewTask && (
        <NewTaskModal
          key={editingJob?.id || 'new'}
          open={showNewTask}
          agents={agents}
          error={modalError}
          onClose={closeModal}
          onSave={saveJob}
          editJob={editingJob}
        />
      )}
    </div>
  );
}
