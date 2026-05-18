import type {
  SubmitTaskRequest,
  ReplyRequest,
  ApiResponse,
  WorkspaceAgent,
  WorkspaceSkill,
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
}

export const api = new ApiClient();
export default api;
