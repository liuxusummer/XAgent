import { useState, useCallback, useRef, useEffect } from 'react';
import type { Message, ChatSession, AgentStatus, ToolCall } from '../types';
import api from '../api/client';

function generateId(): string {
  return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

function createMessage(role: Message['role'], content: string, overrides?: Partial<Message>): Message {
  return {
    id: generateId(),
    role,
    content,
    timestamp: Date.now(),
    status: 'complete',
    ...overrides,
  };
}

function ensureAgentMessage(messages: Message[]): Message {
  const lastMsg = messages[messages.length - 1];
  if (lastMsg && lastMsg.role === 'agent' && lastMsg.status === 'streaming') {
    return lastMsg;
  }
  const message = createMessage('agent', '', { status: 'streaming' });
  messages.push(message);
  return message;
}

function appendDelta(current: string, delta: string): string {
  if (!delta || current.endsWith(delta)) return current;
  return current + delta;
}

function toolKey(tool: Pick<ToolCall, 'name' | 'arguments'>): string {
  return `${tool.name}:${JSON.stringify(tool.arguments || {})}`;
}

function appendToolCall(toolCalls: ToolCall[] | undefined, toolCall: ToolCall): ToolCall[] {
  const current = toolCalls || [];
  if (current.some(tool => tool.id === toolCall.id || toolKey(tool) === toolKey(toolCall))) {
    return current;
  }
  return [...current, toolCall];
}

export function useChat() {
  const [session, setSession] = useState<ChatSession>({
    id: generateId(),
    title: 'New Chat',
    messages: [],
    createdAt: Date.now(),
    updatedAt: Date.now(),
    status: 'idle',
  });

  const [agentStatus, setAgentStatus] = useState<AgentStatus>({
    state: 'idle',
  });

  const [isWaitingForUser, setIsWaitingForUser] = useState(false);
  const [askPrompt, setAskPrompt] = useState('');
  const eventSourceRef = useRef<EventSource | null>(null);
  const backendSessionIdRef = useRef<string>('');
  const streamRunIdRef = useRef(0);

  const closeEventSource = useCallback(() => {
    streamRunIdRef.current += 1;
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  const addMessage = useCallback((message: Message) => {
    setSession(prev => ({
      ...prev,
      messages: [...prev.messages, message],
      updatedAt: Date.now(),
    }));
  }, []);

  const handleSSEMessage = useCallback((event: MessageEvent) => {
    try {
      const data = JSON.parse(event.data);

      switch (data.type) {
        case 'assistant_delta': {
          const content = data.data as string;
          setSession(prev => {
            const messages = [...prev.messages];
            const agentMsg = ensureAgentMessage(messages);
            agentMsg.content = appendDelta(agentMsg.content, content);

            return { ...prev, messages, status: 'running' as const };
          });
          break;
        }

        case 'thinking_delta': {
          const content = data.data as string;
          setSession(prev => {
            const messages = [...prev.messages];
            const agentMsg = ensureAgentMessage(messages);
            agentMsg.thinking = appendDelta(agentMsg.thinking || '', content);

            return { ...prev, messages };
          });
          break;
        }

        case 'ask_user': {
          const prompt = data.data as string;
          setIsWaitingForUser(true);
          setAskPrompt(prompt);
          setAgentStatus({ state: 'waiting_for_user' });
          setSession(prev => ({ ...prev, status: 'waiting_for_user' }));

          addMessage(createMessage('system', `[Agent asks] ${prompt}`));
          break;
        }

        case 'tool_call': {
          const toolCall = data.data as ToolCall;
          setAgentStatus(prev => ({
            ...prev,
            state: 'executing',
            currentTool: toolCall.name,
          }));

          setSession(prev => {
            const messages = [...prev.messages];
            const agentMsg = ensureAgentMessage(messages);
            agentMsg.toolCalls = appendToolCall(agentMsg.toolCalls, toolCall);

            return { ...prev, messages };
          });
          break;
        }

        case 'tool_result': {
          const { toolId, result } = data.data as { toolId: string; result: unknown };

          setSession(prev => {
            const messages = prev.messages.map(msg => {
              if (!msg.toolCalls) return msg;
              return {
                ...msg,
                toolCalls: msg.toolCalls.map(tc =>
                  tc.id === toolId
                    ? { ...tc, status: 'success' as const, result }
                    : tc
                ),
              };
            });
            return { ...prev, messages };
          });
          break;
        }

        case 'turn_start': {
          const turn = (data.data as { turn?: number })?.turn;
          setAgentStatus(prev => ({
            ...prev,
            state: 'thinking',
            currentTurn: turn,
          }));
          setSession(prev => {
            const messages = [...prev.messages];
            const agentMsg = ensureAgentMessage(messages);
            agentMsg.metadata = {
              ...agentMsg.metadata,
              turn,
            };
            return { ...prev, messages, status: 'running' as const };
          });
          break;
        }

        case 'log':
        case 'user_reply': {
          break;
        }

        case 'run_done': {
          const result = data.data as { exit_reason?: string; turns?: number };
          setAgentStatus(prev => ({
            ...prev,
            maxTurns: result.turns || prev.maxTurns,
          }));
          break;
        }

        case 'done': {
          const result = data.data as {
            response?: string;
            exit_reason?: string;
            tool_results?: Array<{
              tool_name?: string;
              tool_call_id?: string;
              data?: unknown;
            }>;
          };

          setAgentStatus({ state: 'idle' });
          setSession(prev => ({ ...prev, status: 'idle' }));

          setSession(prev => {
            const messages = [...prev.messages];
            const lastMsg = messages[messages.length - 1];

            if (lastMsg && lastMsg.role === 'agent') {
              if (!lastMsg.content && result.response) {
                lastMsg.content = result.response;
              }
              if (result.tool_results?.length) {
                const fallbackTools = result.tool_results
                  .filter(tool => tool.tool_name && tool.tool_name !== 'no_tool')
                  .map(tool => ({
                    id: tool.tool_call_id || tool.tool_name!,
                    name: tool.tool_name!,
                    arguments: {},
                    status: 'success' as const,
                    result: tool.data,
                  }));
                if (fallbackTools.length) {
                  lastMsg.toolCalls = fallbackTools.reduce(
                    (toolCalls, toolCall) => appendToolCall(toolCalls, toolCall),
                    lastMsg.toolCalls || []
                  );
                }
              }
              lastMsg.status = 'complete';
              lastMsg.metadata = {
                ...lastMsg.metadata,
                exitReason: result.exit_reason || '',
              };
            } else if (result.response) {
              messages.push(createMessage('agent', result.response, {
                metadata: { exitReason: result.exit_reason || '' },
              }));
            }

            return { ...prev, messages, status: 'idle' as const };
          });

          closeEventSource();
          break;
        }

        case 'error': {
          const error = data.data as string;
          setAgentStatus({ state: 'error' });
          setSession(prev => ({ ...prev, status: 'error' }));
          addMessage(createMessage('system', `[Error] ${error}`, { status: 'error' }));
          closeEventSource();
          break;
        }
      }
    } catch (err) {
      console.error('Failed to parse SSE message:', err);
    }
  }, [addMessage, closeEventSource]);

  const submitTask = useCallback(async (task: string, config?: {
    configPath?: string;
    observabilityConfigPath?: string;
    workspaceDir?: string;
  }) => {
    if (!task.trim()) return;

    // Close any existing connection
    closeEventSource();

    // Add user message
    addMessage(createMessage('user', task.trim()));

    // Update session status
    setSession(prev => ({
      ...prev,
      status: 'running',
      config: config || prev.config,
    }));
    setAgentStatus({ state: 'thinking' });
    setIsWaitingForUser(false);
    setAskPrompt('');

    try {
      // Submit task to API
      const response = await api.submitTask({
        task: task.trim(),
        session_id: backendSessionIdRef.current || undefined,
        config_path: config?.configPath,
        observability_config_path: config?.observabilityConfigPath,
        workspace_dir: config?.workspaceDir,
      });

      if (!response.success) {
        addMessage(createMessage('system', `[Error] Failed to submit task: ${response.error}`, {
          status: 'error',
        }));
        setAgentStatus({ state: 'idle' });
        setSession(prev => ({ ...prev, status: 'idle' }));
        return;
      }

      backendSessionIdRef.current = response.data?.session_id || '';
      const streamRunId = streamRunIdRef.current + 1;
      streamRunIdRef.current = streamRunId;

      // Connect to SSE stream
      const es = api.createEventSource(backendSessionIdRef.current);
      eventSourceRef.current = es;

      es.onmessage = event => {
        if (streamRunId !== streamRunIdRef.current) return;
        handleSSEMessage(event);
      };

      es.onerror = () => {
        if (streamRunId !== streamRunIdRef.current) return;
        console.error('SSE connection error');
        setAgentStatus({ state: 'error' });
        setSession(prev => ({ ...prev, status: 'error' }));
        closeEventSource();
      };

      es.onopen = () => {
        console.debug('SSE connection opened');
      };

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      addMessage(createMessage('system', `[Error] ${errorMsg}`, { status: 'error' }));
      setAgentStatus({ state: 'idle' });
      setSession(prev => ({ ...prev, status: 'idle' }));
    }
  }, [addMessage, closeEventSource, handleSSEMessage]);

  const sendReply = useCallback(async (reply: string) => {
    if (!reply.trim() || !isWaitingForUser) return;

    addMessage(createMessage('user', reply.trim()));
    setIsWaitingForUser(false);
    setAskPrompt('');
    setAgentStatus({ state: 'thinking' });
    setSession(prev => ({ ...prev, status: 'running' }));

    try {
      const response = await api.sendReply({
        reply: reply.trim(),
        session_id: backendSessionIdRef.current,
      });

      if (!response.success) {
        addMessage(createMessage('system', `[Error] Failed to send reply: ${response.error}`, {
          status: 'error',
        }));
        setAgentStatus({ state: 'idle' });
        setSession(prev => ({ ...prev, status: 'idle' }));
        return;
      }

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Unknown error';
      addMessage(createMessage('system', `[Error] ${errorMsg}`, { status: 'error' }));
      setAgentStatus({ state: 'idle' });
      setSession(prev => ({ ...prev, status: 'idle' }));
    }
  }, [addMessage, isWaitingForUser]);

  const stopTask = useCallback(async () => {
    try {
      await api.stopTask(backendSessionIdRef.current);
      closeEventSource();
      setAgentStatus({ state: 'idle' });
      setSession(prev => ({ ...prev, status: 'idle' }));
      addMessage(createMessage('system', '[Stop] Task interrupted'));
    } catch (error) {
      console.error('Failed to stop task:', error);
    }
  }, [addMessage, closeEventSource]);

  const clearChat = useCallback(() => {
    closeEventSource();
    backendSessionIdRef.current = '';
    setSession({
      id: generateId(),
      title: 'New Chat',
      messages: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
      status: 'idle',
    });
    setAgentStatus({ state: 'idle' });
    setIsWaitingForUser(false);
    setAskPrompt('');
  }, [closeEventSource]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      closeEventSource();
    };
  }, [closeEventSource]);

  return {
    session,
    agentStatus,
    isWaitingForUser,
    askPrompt,
    submitTask,
    sendReply,
    stopTask,
    clearChat,
  };
}
