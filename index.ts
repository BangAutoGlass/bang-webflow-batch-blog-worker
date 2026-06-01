// Compatibility entrypoint.
// The Render start command runs src/index.ts directly, but this file is kept
// so older manual commands such as `tsx index.ts` still start the worker.
import "./src/index.ts"
