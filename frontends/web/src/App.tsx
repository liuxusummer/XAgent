import { useState } from 'react';
import { Sidebar } from './components/Sidebar';
import { ChatArea } from './components/ChatArea';
import { SettingsModal, loadConfig, type AgentConfig } from './components/SettingsModal';
import { useChat } from './hooks/useChat';
import { ThemeProvider } from './hooks/useTheme.tsx';

function App() {
  const {
    session,
    agentStatus,
    isWaitingForUser,
    askPrompt,
    submitTask,
    sendReply,
    stopTask,
    clearChat,
  } = useChat();

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [agentConfig, setAgentConfig] = useState<AgentConfig>(loadConfig());

  const handleSaveConfig = (config: AgentConfig) => {
    setAgentConfig(config);
  };

  const handleSubmitTask = (task: string) => {
    submitTask(task, agentConfig);
  };

  return (
    <ThemeProvider>
      <div className="flex h-screen w-screen bg-bg-primary">
        <Sidebar
          onNewChat={clearChat}
          onClearChat={clearChat}
          currentSessionId={session.id}
          onOpenSettings={() => setSettingsOpen(true)}
        />
        <ChatArea
          messages={session.messages}
          agentStatus={agentStatus}
          isWaitingForUser={isWaitingForUser}
          askPrompt={askPrompt}
          onSubmitTask={handleSubmitTask}
          onSendReply={sendReply}
          onStopTask={stopTask}
        />
        <SettingsModal
          isOpen={settingsOpen}
          onClose={() => setSettingsOpen(false)}
          onSave={handleSaveConfig}
          initialConfig={agentConfig}
        />
      </div>
    </ThemeProvider>
  );
}

export default App;
