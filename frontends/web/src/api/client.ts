import type {
  SubmitTaskRequest,
  ReplyRequest,
  ApiResponse,
  WorkspaceAgent,
  WorkspaceCreateRequest,
  WorkspaceCreateResponse,
  WorkspaceSkill,
  WorkspaceTemplate,
  WorkspaceTool,
  WorkspaceFile,
  WorkspaceFileWriteResponse,
  WorkspaceTreeNode,
  WorkspaceIndexStats,
  WorkspaceIndexRefreshResult,
  WorkspaceIndexSearchResult,
  WorkspacePreviewFile,
  EvalDataset,
  EvalDatasetDownloadRequest,
  EvalDatasetImportRequest,
  EvalRunCreateRequest,
  EvalRunResult,
  AgentProfile,
  AgentProfileData,
  MemoryEntry,
  ChatMetadata,
  PersistentChatDetail,
  UsageSummary,
  TraceSessionDetail,
  TraceSessionList,
  ScheduledTask,
  ScheduledTaskWriteRequest,
  AgentTeam,
  AgentTeamWorkflow,
} from '../types';

const API_BASE = import.meta.env.VITE_API_BASE || '';

class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
  }

  private async fetch<T>(path: string, options?: RequestInit): Promise<ApiResponse<T>> {
    try {
      const url = this.baseUrl ? `${this.baseUrl}${path}` : path;
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        return {
          success: false,
          error: `HTTP ${response.status}: ${errorText || response.statusText}`,
        };
      }

      return await response.json() as ApiResponse<T>;
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Network error',
      };
    }
  }

  async submitTask(request: SubmitTaskRequest): Promise<ApiResponse<{ session_id: string }>> {
    return this.fetch('/api/chat', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async sendReply(request: ReplyRequest): Promise<ApiResponse<void>> {
    return this.fetch('/api/chat/reply', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async stopTask(sessionId?: string): Promise<ApiResponse<void>> {
    return this.fetch('/api/chat/stop', {
      method: 'POST',
      body: JSON.stringify({ session_id: sessionId || '' }),
    });
  }

  async listScheduledTasks(ws: string): Promise<ApiResponse<ScheduledTask[]>> {
    return this.fetch(`/api/tasks?ws=${encodeURIComponent(ws)}`);
  }

  async createScheduledTask(request: ScheduledTaskWriteRequest): Promise<ApiResponse<ScheduledTask>> {
    return this.fetch('/api/tasks', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async updateScheduledTask(id: string, request: ScheduledTaskWriteRequest): Promise<ApiResponse<ScheduledTask>> {
    return this.fetch(`/api/tasks/${encodeURIComponent(id)}`, {
      method: 'PUT',
      body: JSON.stringify(request),
    });
  }

  async setScheduledTaskStatus(
    ws: string,
    id: string,
    status: ScheduledTask['status']
  ): Promise<ApiResponse<ScheduledTask>> {
    return this.fetch(`/api/tasks/${encodeURIComponent(id)}/status`, {
      method: 'POST',
      body: JSON.stringify({ ws, status }),
    });
  }

  async debugRunScheduledTask(ws: string, id: string): Promise<ApiResponse<ScheduledTask>> {
    return this.fetch(`/api/tasks/${encodeURIComponent(id)}/run`, {
      method: 'POST',
      body: JSON.stringify({ ws }),
    });
  }

  async deleteScheduledTask(ws: string, id: string): Promise<ApiResponse<void>> {
    return this.fetch(`/api/tasks/${encodeURIComponent(id)}?ws=${encodeURIComponent(ws)}`, {
      method: 'DELETE',
    });
  }

  async listChats(ws: string, agent: string): Promise<ApiResponse<ChatMetadata[]>> {
    const params = new URLSearchParams({ ws, agent });
    return this.fetch(`/api/chats?${params.toString()}`);
  }

  async createChat(ws: string, agent: string): Promise<ApiResponse<PersistentChatDetail>> {
    return this.fetch('/api/chats', {
      method: 'POST',
      body: JSON.stringify({ ws, agent }),
    });
  }

  async readChat(ws: string, agent: string, chatId: string): Promise<ApiResponse<PersistentChatDetail>> {
    const params = new URLSearchParams({ ws, agent });
    return this.fetch(`/api/chats/${encodeURIComponent(chatId)}?${params.toString()}`);
  }

  async deleteChat(ws: string, agent: string, chatId: string): Promise<ApiResponse<void>> {
    const params = new URLSearchParams({ ws, agent });
    return this.fetch(`/api/chats/${encodeURIComponent(chatId)}?${params.toString()}`, {
      method: 'DELETE',
    });
  }

  createEventSource(sessionId?: string): EventSource {
    const path = sessionId
      ? `/api/chat/stream?session_id=${sessionId}`
      : '/api/chat/stream';
    const url = this.baseUrl ? `${this.baseUrl}${path}` : path;
    return new EventSource(url);
  }

  async listWorkspaces(): Promise<ApiResponse<string[]>> {
    return this.fetch('/api/workspace/list');
  }

  async listWorkspaceTemplates(): Promise<ApiResponse<WorkspaceTemplate[]>> {
    return this.fetch('/api/workspace/templates');
  }

  async createWorkspace(request: WorkspaceCreateRequest): Promise<ApiResponse<WorkspaceCreateResponse>> {
    return this.fetch('/api/workspace', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async listAgents(ws: string = 'default.ws'): Promise<ApiResponse<WorkspaceAgent[]>> {
    return this.fetch(`/api/workspace/agents?ws=${encodeURIComponent(ws)}`);
  }

  async listSkills(ws: string = 'default.ws'): Promise<ApiResponse<WorkspaceSkill[]>> {
    return this.fetch(`/api/workspace/skills?ws=${encodeURIComponent(ws)}`);
  }

  async listTools(): Promise<ApiResponse<WorkspaceTool[]>> {
    return this.fetch('/api/workspace/tools');
  }

  async readWorkspaceFile(ws: string, path: string): Promise<ApiResponse<WorkspaceFile>> {
    return this.fetch(`/api/workspace/file?ws=${encodeURIComponent(ws)}&path=${encodeURIComponent(path)}`);
  }

  async writeWorkspaceFile(ws: string, path: string, content: string): Promise<ApiResponse<WorkspaceFileWriteResponse>> {
    return this.fetch('/api/workspace/file', {
      method: 'PUT',
      body: JSON.stringify({ ws, path, content }),
    });
  }

  async readAgentProfile(ws: string, agent: string): Promise<ApiResponse<AgentProfileData>> {
    return this.fetch(`/api/workspace/agent-profile?ws=${encodeURIComponent(ws)}&agent=${encodeURIComponent(agent)}`);
  }

  async writeAgentProfile(
    ws: string,
    agent: string,
    profile: AgentProfile,
    body: string
  ): Promise<ApiResponse<AgentProfileData>> {
    return this.fetch('/api/workspace/agent-profile', {
      method: 'PUT',
      body: JSON.stringify({ ws, agent, profile, body }),
    });
  }

  async listTeams(ws: string = 'default.ws'): Promise<ApiResponse<AgentTeam[]>> {
    return this.fetch(`/api/workspace/teams?ws=${encodeURIComponent(ws)}`);
  }

  async readTeam(ws: string, team: string): Promise<ApiResponse<AgentTeam>> {
    return this.fetch(`/api/workspace/team?ws=${encodeURIComponent(ws)}&team=${encodeURIComponent(team)}`);
  }

  async writeTeam(ws: string, team: AgentTeam): Promise<ApiResponse<AgentTeam>> {
    return this.fetch('/api/workspace/team', {
      method: 'PUT',
      body: JSON.stringify({ ws, team }),
    });
  }

  async deleteTeam(ws: string, team: string): Promise<ApiResponse<void>> {
    return this.fetch(`/api/workspace/team?ws=${encodeURIComponent(ws)}&team=${encodeURIComponent(team)}`, {
      method: 'DELETE',
    });
  }

  async readTeamWorkflow(ws: string, team: string): Promise<ApiResponse<AgentTeamWorkflow | null>> {
    return this.fetch(`/api/workspace/team/workflow?ws=${encodeURIComponent(ws)}&team=${encodeURIComponent(team)}`);
  }

  async writeTeamWorkflow(
    ws: string,
    team: string,
    workflow: AgentTeamWorkflow
  ): Promise<ApiResponse<AgentTeamWorkflow>> {
    return this.fetch('/api/workspace/team/workflow', {
      method: 'PUT',
      body: JSON.stringify({ ws, team, workflow }),
    });
  }

  async listMemoryEntries(ws: string): Promise<ApiResponse<MemoryEntry[]>> {
    return this.fetch(`/api/workspace/memory?ws=${encodeURIComponent(ws)}`);
  }

  async createMemoryEntry(ws: string, entry: Omit<MemoryEntry, 'id' | 'createdAt'>): Promise<ApiResponse<MemoryEntry>> {
    return this.fetch('/api/workspace/memory', {
      method: 'POST',
      body: JSON.stringify({ ws, ...entry }),
    });
  }

  async deleteMemoryEntry(ws: string, id: string): Promise<ApiResponse<void>> {
    return this.fetch(`/api/workspace/memory?ws=${encodeURIComponent(ws)}&id=${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  async deleteWorkspaceFile(ws: string, path: string): Promise<ApiResponse<void>> {
    return this.fetch(`/api/workspace/file?ws=${encodeURIComponent(ws)}&path=${encodeURIComponent(path)}`, {
      method: 'DELETE',
    });
  }

  async getWorkspaceTree(ws: string): Promise<ApiResponse<WorkspaceTreeNode>> {
    return this.fetch(`/api/workspace/tree?ws=${encodeURIComponent(ws)}`);
  }

  async getWorkspaceIndexStats(ws: string): Promise<ApiResponse<WorkspaceIndexStats>> {
    return this.fetch(`/api/workspace/index/stats?ws=${encodeURIComponent(ws)}`);
  }

  async refreshWorkspaceIndex(ws: string, root: string = ''): Promise<ApiResponse<WorkspaceIndexRefreshResult>> {
    return this.fetch('/api/workspace/index/refresh', {
      method: 'POST',
      body: JSON.stringify({ ws, root }),
    });
  }

  async searchWorkspaceIndex(
    ws: string,
    query: string,
    options: { root?: string; limit?: number; refresh?: boolean; pathOnly?: boolean } = {}
  ): Promise<ApiResponse<WorkspaceIndexSearchResult>> {
    const params = new URLSearchParams({
      ws,
      q: query,
      root: options.root || '',
      limit: String(options.limit ?? 20),
      refresh: String(options.refresh ?? false),
      path_only: String(options.pathOnly ?? false),
    });
    return this.fetch(`/api/workspace/index/search?${params.toString()}`);
  }

  async previewWorkspaceFile(ws: string, path: string): Promise<ApiResponse<WorkspacePreviewFile>> {
    return this.fetch(`/api/workspace/preview?ws=${encodeURIComponent(ws)}&path=${encodeURIComponent(path)}`);
  }

  async listEvalDatasets(ws: string): Promise<ApiResponse<EvalDataset[]>> {
    return this.fetch(`/api/eval/datasets?ws=${encodeURIComponent(ws)}`);
  }

  async importEvalDataset(request: EvalDatasetImportRequest): Promise<ApiResponse<EvalDataset>> {
    return this.fetch('/api/eval/datasets/import', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async downloadEvalDataset(request: EvalDatasetDownloadRequest): Promise<ApiResponse<EvalDataset>> {
    return this.fetch('/api/eval/datasets/download', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async getEvalDataset(ws: string, datasetId: string): Promise<ApiResponse<EvalDataset>> {
    return this.fetch(`/api/eval/datasets/${encodeURIComponent(datasetId)}?ws=${encodeURIComponent(ws)}`);
  }

  async createEvalRun(request: EvalRunCreateRequest): Promise<ApiResponse<EvalRunResult>> {
    return this.fetch('/api/eval/runs', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async listEvalRuns(ws: string): Promise<ApiResponse<EvalRunResult[]>> {
    return this.fetch(`/api/eval/runs?ws=${encodeURIComponent(ws)}`);
  }

  async getEvalRun(ws: string, runId: string): Promise<ApiResponse<EvalRunResult>> {
    return this.fetch(`/api/eval/runs/${encodeURIComponent(runId)}?ws=${encodeURIComponent(ws)}`);
  }

  async cancelEvalRun(ws: string, runId: string): Promise<ApiResponse<EvalRunResult>> {
    return this.fetch(`/api/eval/runs/${encodeURIComponent(runId)}/cancel?ws=${encodeURIComponent(ws)}`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
  }

  async getUsageSummary(options: { ws?: string; observabilityConfigPath?: string; limit?: number } = {}): Promise<ApiResponse<UsageSummary>> {
    const params = new URLSearchParams({
      ws: options.ws || 'default.ws',
      observability_config_path: options.observabilityConfigPath || '',
      limit: String(options.limit ?? 30),
    });
    return this.fetch(`/api/usage/summary?${params.toString()}`);
  }

  async listTraceSessions(options: { ws?: string; observabilityConfigPath?: string; limit?: number } = {}): Promise<ApiResponse<TraceSessionList>> {
    const params = new URLSearchParams({
      ws: options.ws || 'default.ws',
      observability_config_path: options.observabilityConfigPath || '',
      limit: String(options.limit ?? 30),
    });
    return this.fetch(`/api/trace/sessions?${params.toString()}`);
  }

  async getTraceSession(
    sessionId: string,
    options: { ws?: string; observabilityConfigPath?: string } = {}
  ): Promise<ApiResponse<TraceSessionDetail>> {
    const params = new URLSearchParams({
      ws: options.ws || 'default.ws',
      observability_config_path: options.observabilityConfigPath || '',
    });
    return this.fetch(`/api/trace/sessions/${encodeURIComponent(sessionId)}?${params.toString()}`);
  }
}

export const api = new ApiClient();
export default api;
