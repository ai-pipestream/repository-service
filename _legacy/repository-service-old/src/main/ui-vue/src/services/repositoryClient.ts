import { createClient } from '@connectrpc/connect'
import { 
  createBinaryTransport,
  PipeDocRepositoryService,
  ProcessRequestRepositoryService,
  create,
  PipeDocSchema,
  ModuleProcessRequestSchema,
  TagsSchema,
  BlobSchema,
  BlobBagSchema,
  SearchMetadataSchema
} from '@pipeline/proto-stubs'

// Type imports for TypeScript type checking only
import type { PipeDoc, ModuleProcessRequest, Tags, Blob, BlobBag, SearchMetadata } from '@pipeline/proto-stubs'

// Create transport to connect through web-proxy using binary format
// The web-proxy will route to repository-service based on the service definitions
const transport = createBinaryTransport()

// Create service clients
export const pipeDocClient = createClient(PipeDocRepositoryService, transport)
export const processRequestClient = createClient(ProcessRequestRepositoryService, transport)

// ============================================================================
// PIPEDOC OPERATIONS
// ============================================================================

/**
 * Create a new PipeDoc in the repository
 */
export async function createPipeDoc(
  pipeDoc: PipeDoc,
  tags?: Tags,
  description?: string
): Promise<{ storageId: string; storedDocument?: any }> {
  const response = await pipeDocClient.createStoredPipeDoc({
    pipeDoc,
    tags,
    description
  }) as any
  return {
    storageId: response.storageId,
    storedDocument: response.storedDocument
  }
}

/**
 * List all stored PipeDocs with metadata
 */
export async function listPipeDocs(
  pageSize = 20,
  pageToken?: string,
  filter?: string
): Promise<{
  documents: any[]
  nextPageToken: string
  totalCount: number
}> {
  const response = await pipeDocClient.listStoredPipeDocs({
    pagination: {
      pageSize,
      pageToken,
      filter
    }
  }) as any
  return {
    documents: response.storedDocuments,
    nextPageToken: response.nextPageToken,
    totalCount: response.totalCount
  }
}

/**
 * Get a specific PipeDoc by storage ID
 */
export async function getPipeDoc(storageId: string) {
  return await pipeDocClient.getStoredPipeDoc({ storageId })
}

/**
 * Update an existing PipeDoc
 */
export async function updatePipeDoc(
  storageId: string,
  pipeDoc: PipeDoc,
  tags?: Tags,
  description?: string
) {
  return await pipeDocClient.updateStoredPipeDoc({
    storageId,
    pipeDoc,
    tags,
    description
  })
}

/**
 * Delete a PipeDoc
 */
export async function deletePipeDoc(storageId: string) {
  return await pipeDocClient.deleteStoredPipeDoc({ storageId })
}

/**
 * Upload a file and create a PipeDoc
 */
export async function uploadFileAsPipeDoc(
  file: File,
  tags?: string[],
  description?: string
): Promise<{ storageId: string; storedDocument?: any }> {
  // Read file content
  const arrayBuffer = await file.arrayBuffer()
  const content = new Uint8Array(arrayBuffer)

  // Create Blob for the file content
  const blob = create(BlobSchema, {
    blobId: crypto.randomUUID(),
    // content is oneof: set via content property
    content: { case: 'data', value: content },
    mimeType: file.type || 'application/octet-stream',
    filename: file.name,
    sizeBytes: BigInt(file.size)
  }) as Blob

  // Create BlobBag containing the Blob
  const blobBag = create(BlobBagSchema, {
    // oneof blobData: set blob
    blobData: { case: 'blob', value: blob }
  }) as BlobBag

  // Create SearchMetadata
  const searchMetadata = create(SearchMetadataSchema, {
    title: file.name,
    sourceUri: `file://${file.name}`,
    sourceMimeType: file.type || 'application/octet-stream',
    contentLength: file.size
  }) as SearchMetadata

  // Create PipeDoc with proper structure (docId will be assigned by the backend)
  const pipeDoc = create(PipeDocSchema, {
    searchMetadata: searchMetadata,
    blobBag: blobBag
  }) as PipeDoc

  // Create Tags if provided
  const tagObject = tags?.length ? create(TagsSchema, { tagData: Object.fromEntries(tags.map(t => [t, ''])) }) as Tags : undefined

  return createPipeDoc(pipeDoc, tagObject, description)
}

// ============================================================================
// MODULE PROCESS REQUEST OPERATIONS
// ============================================================================

/**
 * Create a new ModuleProcessRequest in the repository
 */
export async function createProcessRequest(
  processRequest: ModuleProcessRequest,
  name?: string,
  description?: string,
  tags?: Tags
): Promise<{ storageId: string; storedRequest?: any }> {
  const response = await processRequestClient.createStoredProcessRequest({
    processRequest,
    name,
    description,
    tags
  }) as any
  return {
    storageId: response.storageId,
    storedRequest: response.storedRequest
  }
}

/**
 * List all stored ProcessRequests with metadata
 */
export async function listProcessRequests(
  pageSize = 20,
  pageToken?: string,
  filter?: string
): Promise<{
  requests: any[]
  nextPageToken: string
  totalCount: number
}> {
  const response = await processRequestClient.listStoredProcessRequests({
    pagination: {
      pageSize,
      pageToken,
      filter
    }
  }) as any
  return {
    requests: response.storedRequests,
    nextPageToken: response.nextPageToken,
    totalCount: response.totalCount
  }
}

/**
 * Get a specific ProcessRequest by storage ID
 */
export async function getProcessRequest(storageId: string) {
  return await processRequestClient.getStoredProcessRequest({ storageId })
}

/**
 * Update an existing ProcessRequest
 */
export async function updateProcessRequest(
  storageId: string,
  processRequest: ModuleProcessRequest,
  name?: string,
  description?: string,
  tags?: Tags
) {
  return await processRequestClient.updateStoredProcessRequest({
    storageId,
    processRequest,
    name,
    description,
    tags
  })
}

/**
 * Delete a ProcessRequest
 */
export async function deleteProcessRequest(storageId: string) {
  return await processRequestClient.deleteStoredProcessRequest({ storageId })
}

/**
 * Create a ModuleProcessRequest for a PipeDoc
 */
export async function createProcessRequestForPipeDoc(
  pipeDocStorageId: string,
  moduleName: string,
  configuration?: Record<string, any>,
  name?: string,
  description?: string,
  tags?: string[]
): Promise<{ storageId: string; storedRequest?: any }> {
  // Get the PipeDoc first
  const storedPipeDoc: any = await getPipeDoc(pipeDocStorageId)
  
  // Create ModuleProcessRequest
  const processRequest = create(ModuleProcessRequestSchema, {
    // ModuleProcessRequest fields have changed in v2 schema
    document: storedPipeDoc.document,
    config: configuration ? { customJsonConfig: configuration } : undefined,
    postMappings: []
  }) as ModuleProcessRequest

  // Create Tags if provided
  const tagObject = tags?.length ? create(TagsSchema, { tagData: Object.fromEntries(tags.map(t => [t, ''])) }) as Tags : undefined

  return createProcessRequest(
    processRequest,
    name || `Process ${storedPipeDoc.document?.source?.path} with ${moduleName}`,
    description,
    tagObject
  )
}

// ============================================================================
// BATCH OPERATIONS
// ============================================================================

/**
 * Upload multiple files as PipeDocs
 */
export async function uploadMultipleFiles(
  files: File[],
  defaultTags?: string[],
  description?: string,
  progressCallback?: (current: number, total: number) => void
): Promise<Array<{ file: File; result: { storageId: string } | { error: string } }>> {
  const results: Array<{ file: File; result: { storageId: string } | { error: string } }> = []

  for (let i = 0; i < files.length; i++) {
    const file = files[i]
    progressCallback?.(i + 1, files.length)

    try {
      const { storageId } = await uploadFileAsPipeDoc(file, defaultTags, description)
      results.push({ file, result: { storageId } })
    } catch (error: any) {
      results.push({ 
        file, 
        result: { error: error.message || 'Failed to upload' } 
      })
    }
  }

  return results
}

/**
 * Create multiple ProcessRequests for a batch of PipeDocs
 */
export async function createBatchProcessRequests(
  pipeDocStorageIds: string[],
  moduleName: string,
  configuration?: Record<string, any>,
  progressCallback?: (current: number, total: number) => void
): Promise<Array<{ pipeDocId: string; result: { storageId: string } | { error: string } }>> {
  const results: Array<{ pipeDocId: string; result: { storageId: string } | { error: string } }> = []

  for (let i = 0; i < pipeDocStorageIds.length; i++) {
    const pipeDocId = pipeDocStorageIds[i]
    progressCallback?.(i + 1, pipeDocStorageIds.length)

    try {
      const { storageId } = await createProcessRequestForPipeDoc(
        pipeDocId,
        moduleName,
        configuration
      )
      results.push({ pipeDocId, result: { storageId } })
    } catch (error: any) {
      results.push({ 
        pipeDocId, 
        result: { error: error.message || 'Failed to create process request' } 
      })
    }
  }

  return results
}