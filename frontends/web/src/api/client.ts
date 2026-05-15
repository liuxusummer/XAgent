import type {
  SubmitTaskRequest,
  ReplyRequest,
  ApiResponse,
  WorkspaceAgent,
  WorkspaceSkill,
  WorkspaceTool,
  WorkspaceFile,
  WorkspaceFileWriteResponse,
  AgentProfile,
  AgentProfileData,
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
}

export const api = new ApiClient();
export default api;
