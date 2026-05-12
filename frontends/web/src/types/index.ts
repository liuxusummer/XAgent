export interface Message {
  id: string;
  role: 'user' | 'agent' | 'system';
  content: string;
  timestamp: number;
  status?: 'sending' | 'streaming' | 'complete' | 'error' | 'progress';
  toolCalls?: ToolCall[];
  thinking?: string;
  metadata?: {
    exitReason?: string;
    turn?: number;
  };
}

export interface ToolCall {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
  status: 'pending' | 'running' | 'success' | 'error';
  result?: unknown;
  duration?: number;
}

export interface ChatSession {
  id: string;
  title: string;
  messages: Message[];
  createdAt: number;
  updatedAt: number;
  status: 'idle' | 'running' | 'waiting_for_user' | 'error';
  config?: {
    configPath?: string;
    observabilityConfigPath?: string;
    workspaceDir?: string;
  };
}

export interface AgentStatus {
  state: 'idle' | 'thinking' | 'executing' | 'waiting_for_user' | 'error';
  currentTurn?: number;
  maxTurns?: number;
  currentTool?: string;
}

export interface SSEEvent {
  type:
    | 'user_task'
    | 'assistant_delta'
    | 'thinking_delta'
    | 'turn_start'
    | 'tool_call'
    | 'tool_result'
    | 'ask_user'
    | 'user_reply'
    | 'run_done'
    | 'done'
    | 'log'
    | 'stop'
    | 'error'
    | 'heartbeat';
  data: unknown;
}

export interface SubmitTaskRequest {
  task: string;
  session_id?: string;
  config_path?: string;
  observability_config_path?: string;
  workspace_dir?: string;
}

export interface ReplyRequest {
  reply: string;
  session_id?: string;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}
