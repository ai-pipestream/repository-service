<template>
  <div>
    <v-row align="center" class="mb-4">
      <v-col>
        <h1 class="text-h4">Process Requests</h1>
      </v-col>
      <v-col cols="auto">
        <v-btn 
          color="primary" 
          prepend-icon="mdi-plus"
          @click="showCreateDialog = true"
        >
          Create Request
        </v-btn>
      </v-col>
    </v-row>

    <!-- Filters -->
    <v-card class="mb-4">
      <v-card-text>
        <v-row>
          <v-col cols="12" md="4">
            <v-select
              v-model="filterStatus"
              label="Status"
              :items="statusOptions"
              variant="outlined"
              density="compact"
              clearable
            ></v-select>
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="filterModule"
              label="Module"
              :items="moduleOptions"
              variant="outlined"
              density="compact"
              clearable
            ></v-select>
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field
              v-model="searchQuery"
              label="Search requests..."
              prepend-inner-icon="mdi-magnify"
              variant="outlined"
              density="compact"
              clearable
            ></v-text-field>
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <!-- Requests Table -->
    <v-card>
      <v-data-table
        :headers="headers"
        :items="processRequests"
        :items-per-page="10"
        :loading="loading"
      >
        <template v-slot:item.status="{ item }">
          <v-chip :color="getStatusColor(item.status)" size="small">
            {{ item.status }}
          </v-chip>
        </template>
        <template v-slot:item.createdAt="{ item }">
          {{ formatDate(item.createdAt) }}
        </template>
        <template v-slot:item.progress="{ item }">
          <v-progress-linear
            :model-value="item.progress"
            :color="getProgressColor(item.progress)"
            height="20"
          >
            {{ item.progress }}%
          </v-progress-linear>
        </template>
        <template v-slot:item.actions="{ item }">
          <v-btn icon="mdi-eye" size="small" variant="text" @click="viewRequest(item)"></v-btn>
          <v-btn 
            v-if="item.status === 'pending'" 
            icon="mdi-play" 
            size="small" 
            variant="text" 
            color="success"
            @click="startRequest(item)"
          ></v-btn>
          <v-btn 
            v-if="item.status === 'running'" 
            icon="mdi-stop" 
            size="small" 
            variant="text" 
            color="warning"
            @click="stopRequest(item)"
          ></v-btn>
          <v-btn icon="mdi-delete" size="small" variant="text" color="error" @click="deleteRequest(item)"></v-btn>
        </template>
      </v-data-table>
    </v-card>

    <!-- Create Request Dialog -->
    <v-dialog v-model="showCreateDialog" max-width="800">
      <v-card>
        <v-card-title>Create Process Request</v-card-title>
        <v-card-text>
          <v-form ref="createForm">
            <v-text-field
              v-model="newRequest.name"
              label="Request Name"
              variant="outlined"
              density="compact"
              required
            ></v-text-field>
            
            <v-select
              v-model="newRequest.module"
              label="Processing Module"
              :items="availableModules"
              variant="outlined"
              density="compact"
              required
            ></v-select>

            <v-select
              v-model="newRequest.documents"
              label="Select Documents"
              :items="availableDocuments"
              variant="outlined"
              density="compact"
              multiple
              chips
              required
            ></v-select>

            <v-textarea
              v-model="newRequest.configuration"
              label="Module Configuration (JSON)"
              variant="outlined"
              density="compact"
              rows="5"
              placeholder='{"key": "value"}'
            ></v-textarea>

            <v-checkbox
              v-model="newRequest.autoStart"
              label="Start processing immediately"
            ></v-checkbox>
          </v-form>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn variant="text" @click="showCreateDialog = false">Cancel</v-btn>
          <v-btn color="primary" variant="flat" @click="createRequest">Create</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <!-- View Request Dialog -->
    <v-dialog v-model="showViewDialog" max-width="900">
      <v-card v-if="selectedRequest">
        <v-card-title>
          {{ selectedRequest.name }}
          <v-chip :color="getStatusColor(selectedRequest.status)" size="small" class="ml-2">
            {{ selectedRequest.status }}
          </v-chip>
        </v-card-title>
        <v-card-text>
          <v-tabs v-model="viewTab">
            <v-tab value="details">Details</v-tab>
            <v-tab value="configuration">Configuration</v-tab>
            <v-tab value="logs">Logs</v-tab>
            <v-tab value="results">Results</v-tab>
          </v-tabs>

          <v-window v-model="viewTab" class="mt-4">
            <v-window-item value="details">
              <v-list>
                <v-list-item>
                  <v-list-item-title>Module</v-list-item-title>
                  <v-list-item-subtitle>{{ selectedRequest.module }}</v-list-item-subtitle>
                </v-list-item>
                <v-list-item>
                  <v-list-item-title>Created</v-list-item-title>
                  <v-list-item-subtitle>{{ formatDate(selectedRequest.createdAt) }}</v-list-item-subtitle>
                </v-list-item>
                <v-list-item>
                  <v-list-item-title>Documents</v-list-item-title>
                  <v-list-item-subtitle>{{ selectedRequest.documentCount }} documents</v-list-item-subtitle>
                </v-list-item>
                <v-list-item>
                  <v-list-item-title>Progress</v-list-item-title>
                  <v-list-item-subtitle>
                    <v-progress-linear
                      :model-value="selectedRequest.progress"
                      :color="getProgressColor(selectedRequest.progress)"
                      height="25"
                    >
                      {{ selectedRequest.progress }}%
                    </v-progress-linear>
                  </v-list-item-subtitle>
                </v-list-item>
              </v-list>
            </v-window-item>

            <v-window-item value="configuration">
              <pre class="configuration-json">{{ JSON.stringify(selectedRequest.configuration, null, 2) }}</pre>
            </v-window-item>

            <v-window-item value="logs">
              <div class="logs-container">
                <pre>{{ selectedRequest.logs || 'No logs available' }}</pre>
              </div>
            </v-window-item>

            <v-window-item value="results">
              <div v-if="selectedRequest.results">
                <v-alert type="success" class="mb-4">
                  Processing completed successfully
                </v-alert>
                <pre class="results-json">{{ JSON.stringify(selectedRequest.results, null, 2) }}</pre>
              </div>
              <div v-else>
                <v-alert type="info">
                  No results available yet
                </v-alert>
              </div>
            </v-window-item>
          </v-window>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn variant="text" @click="showViewDialog = false">Close</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, inject } from 'vue'
import { 
  createProcessRequestForPipeDoc,
  listProcessRequests,
  deleteProcessRequest,
  listPipeDocs
} from '../services/repositoryClient'

// Inject notification
const showNotification = inject('showNotification') as Function

// State
const loading = ref(false)
const searchQuery = ref('')
const filterStatus = ref('')
const filterModule = ref('')
const viewTab = ref('details')

// Dialogs
const showCreateDialog = ref(false)
const showViewDialog = ref(false)
const selectedRequest = ref<any>(null)

// Form
const newRequest = ref({
  name: '',
  module: '',
  documents: [],
  configuration: '',
  autoStart: false
})

// Data
const processRequests = ref<any[]>([])
const availableDocumentIds = ref<string[]>([])

const statusOptions = ref([
  { title: 'Pending', value: 'pending' },
  { title: 'Running', value: 'running' },
  { title: 'Completed', value: 'completed' },
  { title: 'Failed', value: 'failed' },
  { title: 'Cancelled', value: 'cancelled' }
])

const moduleOptions = ref([
  { title: 'PDF Extractor', value: 'pdf-extractor' },
  { title: 'NLP Analyzer', value: 'nlp-analyzer' },
  { title: 'Classifier', value: 'classifier' },
  { title: 'OCR Processor', value: 'ocr-processor' }
])

const availableModules = ref([
  'pdf-extractor',
  'nlp-analyzer',
  'classifier',
  'ocr-processor',
  'data-transformer'
])

const availableDocuments = ref<Array<{ title: string; value: string }>>([])

const headers = ref<Array<{ title: string; key: string; sortable: boolean; align?: 'start' | 'end' | 'center' }>>([
  { title: 'Name', key: 'name', sortable: true },
  { title: 'Module', key: 'module', sortable: true },
  { title: 'Status', key: 'status', sortable: true },
  { title: 'Progress', key: 'progress', sortable: true },
  { title: 'Documents', key: 'documentCount', sortable: true },
  { title: 'Created', key: 'createdAt', sortable: true },
  { title: 'Actions', key: 'actions', sortable: false, align: 'end' }
])

// Methods
const viewRequest = (request: any) => {
  selectedRequest.value = request
  showViewDialog.value = true
}

const startRequest = (request: any) => {
  request.status = 'running'
  showNotification(`Started processing: ${request.name}`, 'success')
}

const stopRequest = (request: any) => {
  request.status = 'cancelled'
  showNotification(`Stopped processing: ${request.name}`, 'warning')
}

const deleteRequest = async (request: any) => {
  try {
    await deleteProcessRequest(request.storageId || request.id)
    showNotification(`Deleted request: ${request.name}`, 'success')
    await loadProcessRequests()
  } catch (error: any) {
    console.error('Failed to delete request:', error)
    showNotification(`Failed to delete: ${error.message}`, 'error')
  }
}

const createRequest = async () => {
  try {
    loading.value = true
    const configuration = newRequest.value.configuration ? 
      JSON.parse(newRequest.value.configuration) : undefined
    
    // Create process requests for each selected document
    const promises = newRequest.value.documents.map(docId => 
      createProcessRequestForPipeDoc(
        docId,
        newRequest.value.module,
        configuration,
        newRequest.value.name,
        `Process request for ${newRequest.value.module}`
      )
    )
    
    const results = await Promise.all(promises)
    
    showCreateDialog.value = false
    showNotification(`Created ${results.length} process request(s)`, 'success')
    
    // Reset form
    newRequest.value = {
      name: '',
      module: '',
      documents: [],
      configuration: '',
      autoStart: false
    }
    
    // Reload the list
    await loadProcessRequests()
  } catch (error: any) {
    console.error('Failed to create request:', error)
    showNotification(`Failed to create request: ${error.message}`, 'error')
  } finally {
    loading.value = false
  }
}

// Utility functions
const formatDate = (date: Date): string => {
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date)
}

const getStatusColor = (status: string): string => {
  const colors: Record<string, string> = {
    pending: 'grey',
    running: 'info',
    completed: 'success',
    failed: 'error',
    cancelled: 'warning'
  }
  return colors[status] || 'grey'
}

const getProgressColor = (progress: number): string => {
  if (progress < 30) return 'error'
  if (progress < 70) return 'warning'
  return 'success'
}

// Load process requests from repository
const loadProcessRequests = async () => {
  try {
    loading.value = true
    const response = await listProcessRequests(50)
    
    // Transform stored requests to display format
    processRequests.value = response.requests.map(stored => ({
      id: stored.storageId,
      storageId: stored.storageId,
      name: stored.name || 'Unnamed Request',
      module: stored.request?.moduleName || 'Unknown',
      status: 'pending', // Default status
      progress: 0,
      documentCount: 1,
      createdAt: new Date(stored.createdAt?.seconds * 1000 || Date.now()),
      configuration: stored.request?.configuration ? 
        JSON.parse(stored.request.configuration) : {},
      description: stored.description,
      tags: stored.tags?.tags || []
    }))
  } catch (error: any) {
    console.error('Failed to load process requests:', error)
    showNotification('Failed to load process requests', 'error')
  } finally {
    loading.value = false
  }
}

// Load available documents
const loadAvailableDocuments = async () => {
  try {
    const response = await listPipeDocs(100)
    availableDocuments.value = response.documents.map(doc => ({
      title: doc.document?.source?.path || doc.storageId,
      value: doc.storageId
    }))
    availableDocumentIds.value = response.documents.map(doc => doc.storageId)
  } catch (error: any) {
    console.error('Failed to load documents:', error)
    // Use empty list if loading fails
    availableDocuments.value = []
  }
}

// Load data on mount
onMounted(async () => {
  await Promise.all([
    loadProcessRequests(),
    loadAvailableDocuments()
  ])
})
</script>

<style scoped>
.configuration-json,
.results-json {
  background-color: #f5f5f5;
  padding: 16px;
  border-radius: 4px;
  overflow-x: auto;
  font-family: monospace;
  font-size: 14px;
}

.logs-container {
  background-color: #1e1e1e;
  color: #cccccc;
  padding: 16px;
  border-radius: 4px;
  max-height: 400px;
  overflow-y: auto;
  font-family: monospace;
  font-size: 14px;
}

.logs-container pre {
  margin: 0;
  white-space: pre-wrap;
}
</style>