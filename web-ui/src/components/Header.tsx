// Header component

export function Header() {
  return (
    <header className="bg-white dark:bg-gray-800 shadow">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center py-4">
          <div className="flex items-center">
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">FlowMesh</h1>
            <span className="ml-3 text-sm text-gray-500 dark:text-gray-400">Unified Event Fabric</span>
          </div>
        </div>
      </div>
    </header>
  );
}
