/** Host-only Session rules shared by Web and SDK transports; mounts no services. */

export { paginateSessionHistory } from './history-page.ts'
export type { SessionTurnWindow } from './history-page.ts'
export { referencedFile, referencedImage } from './image-references.ts'
export { installModelSelectionProjection } from './model-selection-projection.ts'
export type { ModelSelectionProjectionState, ModelSelectionProjection } from './types.ts'
export { installSessionListMetadata, sessionProjectionHints } from './list.ts'
