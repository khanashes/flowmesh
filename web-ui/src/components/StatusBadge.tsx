// Status badge component

interface StatusBadgeProps {
  status: 'healthy' | 'unhealthy' | 'ready' | 'not-ready' | 'loading' | string;
  label?: string;
}

export function StatusBadge({ status, label }: StatusBadgeProps) {
  const getStatusClasses = (status: string) => {
    switch (status.toLowerCase()) {
      case 'healthy':
      case 'ready':
      case 'active':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400';
      case 'unhealthy':
      case 'not-ready':
      case 'error':
      case 'stopped':
        return 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400';
      case 'loading':
      case 'pending':
      case 'created':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400';
      case 'paused':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
    }
  };

  const displayLabel = label || status;

  return (
    <span
      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusClasses(status)}`}
    >
      <span
        className={`w-1.5 h-1.5 mr-1.5 rounded-full ${
          status.toLowerCase() === 'healthy' || status.toLowerCase() === 'ready'
            ? 'bg-green-500'
            : status.toLowerCase() === 'unhealthy' || status.toLowerCase() === 'error'
            ? 'bg-red-500'
            : 'bg-yellow-500'
        }`}
      />
      {displayLabel}
    </span>
  );
}
