<template>
  <div>
    <h1 class="text-h4 mb-6">Import & Export</h1>

    <v-row>
      <!-- Import Section -->
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-import</v-icon>
            Import Data
          </v-card-title>
          <v-card-text>
            <div class="text-body-2 mb-4">
              Import documents, metadata, and configurations from various sources.
            </div>

            <!-- Import Type Selection -->
            <v-select
              v-model="importType"
              label="Import Type"
              :items="importTypes"
              variant="outlined"
              density="compact"
              class="mb-4"
            ></v-select>

            <!-- File Upload Area -->
            <div
              class="import-drop-zone"
              :class="{ 'import-drop-zone-active': isDragging }"
              @drop="handleDrop"
              @dragover.prevent
              @dragenter.prevent="isDragging = true"
              @dragleave.prevent="isDragging = false"
            >
              <v-icon size="48" color="primary">mdi-cloud-upload</v-icon>
              <div class="text-h6 mt-2">Drop files here</div>
              <div class="text-caption text-grey">or click to browse</div>
              <v-btn
                color="primary"
                variant="outlined"
                class="mt-4"
                @click="onImportBrowseClick"
              >
                Browse Files
              </v-btn>
              <input
                ref="importFileInput"
                type="file"
                multiple
                hidden
                @change="handleFileSelect"
              />
            </div>

            <!-- Import Options -->
            <div v-if="selectedFiles.length > 0" class="mt-4">
              <v-checkbox
                v-model="importOptions.overwrite"
                label="Overwrite existing documents"
                density="compact"
              ></v-checkbox>
              <v-checkbox
                v-model="importOptions.validateSchema"
                label="Validate against schema"
                density="compact"
              ></v-checkbox>
              <v-checkbox
                v-model="importOptions.createBackup"
                label="Create backup before import"
                density="compact"
              ></v-checkbox>
            </div>

            <!-- Selected Files -->
            <v-list v-if="selectedFiles.length > 0" class="mt-4">
              <v-list-subheader>Selected Files ({{ selectedFiles.length }})</v-list-subheader>
              <v-list-item v-for="(file, index) in selectedFiles" :key="index">
                <template v-slot:prepend>
                  <v-icon>mdi-file</v-icon>
                </template>
                <v-list-item-title>{{ file.name }}</v-list-item-title>
                <v-list-item-subtitle>{{ formatBytes(file.size) }}</v-list-item-subtitle>
                <template v-slot:append>
                  <v-btn
                    icon="mdi-close"
                    size="small"
                    variant="text"
                    @click="removeFile(index)"
                  ></v-btn>
                </template>
              </v-list-item>
            </v-list>

            <!-- Import Progress -->
            <v-progress-linear
              v-if="importing"
              :model-value="importProgress"
              color="primary"
              height="25"
              class="mt-4"
            >
              <template v-slot:default="{ value }">
                <strong>{{ Math.ceil(value) }}%</strong>
              </template>
            </v-progress-linear>
          </v-card-text>
          <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn
              color="primary"
              variant="flat"
              :disabled="selectedFiles.length === 0 || importing"
              :loading="importing"
              @click="performImport"
            >
              Import
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>

      <!-- Export Section -->
      <v-col cols="12" md="6">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-export</v-icon>
            Export Data
          </v-card-title>
          <v-card-text>
            <div class="text-body-2 mb-4">
              Export documents, metadata, and configurations to various formats.
            </div>

            <!-- Export Type Selection -->
            <v-select
              v-model="exportType"
              label="Export Type"
              :items="exportTypes"
              variant="outlined"
              density="compact"
              class="mb-4"
            ></v-select>

            <!-- Export Format -->
            <v-select
              v-model="exportFormat"
              label="Export Format"
              :items="exportFormats"
              variant="outlined"
              density="compact"
              class="mb-4"
            ></v-select>

            <!-- Export Options -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Export Options</div>
              <v-checkbox
                v-model="exportOptions.includeMetadata"
                label="Include metadata"
                density="compact"
              ></v-checkbox>
              <v-checkbox
                v-model="exportOptions.includeVersionHistory"
                label="Include version history"
                density="compact"
              ></v-checkbox>
              <v-checkbox
                v-model="exportOptions.compressOutput"
                label="Compress output (ZIP)"
                density="compact"
              ></v-checkbox>
            </div>

            <!-- Date Range Filter -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Date Range</div>
              <v-row>
                <v-col cols="6">
                  <v-text-field
                    v-model="exportDateRange.start"
                    label="Start Date"
                    type="date"
                    variant="outlined"
                    density="compact"
                  ></v-text-field>
                </v-col>
                <v-col cols="6">
                  <v-text-field
                    v-model="exportDateRange.end"
                    label="End Date"
                    type="date"
                    variant="outlined"
                    density="compact"
                  ></v-text-field>
                </v-col>
              </v-row>
            </div>

            <!-- Tag Filter -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Filter by Tags</div>
              <v-combobox
                v-model="exportTags"
                label="Select tags to export"
                variant="outlined"
                density="compact"
                multiple
                chips
                closable-chips
                :items="availableTags"
              ></v-combobox>
            </div>

            <!-- Export Preview -->
            <v-alert v-if="exportPreview" type="info" class="mb-4">
              <div>Export will include:</div>
              <ul class="mt-2">
                <li>{{ exportPreview.documentCount }} documents</li>
                <li>{{ formatBytes(exportPreview.totalSize) }} total size</li>
                <li>Format: {{ exportFormat }}</li>
              </ul>
            </v-alert>

            <!-- Export Progress -->
            <v-progress-linear
              v-if="exporting"
              :model-value="exportProgress"
              color="primary"
              height="25"
              class="mt-4"
            >
              <template v-slot:default="{ value }">
                <strong>{{ Math.ceil(value) }}%</strong>
              </template>
            </v-progress-linear>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="outlined" @click="previewExport">
              Preview
            </v-btn>
            <v-spacer></v-spacer>
            <v-btn
              color="primary"
              variant="flat"
              :loading="exporting"
              @click="performExport"
            >
              Export
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>
    </v-row>

    <!-- Recent Import/Export History -->
    <v-card class="mt-6">
      <v-card-title>
        <v-icon class="mr-2">mdi-history</v-icon>
        Recent Activity
      </v-card-title>
      <v-card-text>
        <v-data-table
          :headers="historyHeaders"
          :items="importExportHistory"
          :items-per-page="5"
        >
          <template v-slot:item.type="{ item }">
            <v-chip :color="item.type === 'import' ? 'primary' : 'success'" size="small">
              {{ item.type }}
            </v-chip>
          </template>
          <template v-slot:item.size="{ item }">
            {{ formatBytes(item.size) }}
          </template>
          <template v-slot:item.createdAt="{ item }">
            {{ formatDate(item.createdAt) }}
          </template>
          <template v-slot:item.status="{ item }">
            <v-chip :color="getStatusColor(item.status)" size="small">
              {{ item.status }}
            </v-chip>
          </template>
          <template v-slot:item.actions="{ item }">
            <v-btn
              v-if="item.type === 'export'"
              icon="mdi-download"
              size="small"
              variant="text"
              @click="downloadExport(item)"
            ></v-btn>
            <v-btn
              icon="mdi-information"
              size="small"
              variant="text"
              @click="viewDetails(item)"
            ></v-btn>
          </template>
        </v-data-table>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { ref, inject } from 'vue'
const importFileInput = ref<HTMLInputElement | null>(null)
const onImportBrowseClick = () => importFileInput.value?.click()

// Inject notification
const showNotification = inject('showNotification') as Function

// Import State
const importType = ref('documents')
const selectedFiles = ref<File[]>([])
const isDragging = ref(false)
const importing = ref(false)
const importProgress = ref(0)
const importOptions = ref({
  overwrite: false,
  validateSchema: true,
  createBackup: true
})

// Export State
const exportType = ref('documents')
const exportFormat = ref('json')
const exporting = ref(false)
const exportProgress = ref(0)
const exportOptions = ref({
  includeMetadata: true,
  includeVersionHistory: false,
  compressOutput: false
})
const exportDateRange = ref({
  start: '',
  end: ''
})
const exportTags = ref<string[]>([])
const exportPreview = ref<any>(null)

// Data
const importTypes = ref([
  { title: 'Documents', value: 'documents' },
  { title: 'Metadata Only', value: 'metadata' },
  { title: 'Full Backup', value: 'backup' },
  { title: 'Configuration', value: 'config' }
])

const exportTypes = ref([
  { title: 'All Documents', value: 'documents' },
  { title: 'Search Results', value: 'search' },
  { title: 'Selected Items', value: 'selected' },
  { title: 'Full Backup', value: 'backup' }
])

const exportFormats = ref([
  { title: 'JSON', value: 'json' },
  { title: 'CSV', value: 'csv' },
  { title: 'XML', value: 'xml' },
  { title: 'Archive (ZIP)', value: 'zip' }
])

const availableTags = ref(['report', 'financial', 'product', 'data', '2023', '2024'])

const importExportHistory = ref([
  {
    id: '1',
    type: 'export',
    name: 'Full_Backup_2024-01-15.zip',
    size: 125678901,
    status: 'completed',
    createdAt: new Date('2024-01-15T10:00:00'),
    documentCount: 1234
  },
  {
    id: '2',
    type: 'import',
    name: 'Documents_Batch_01.json',
    size: 45678901,
    status: 'completed',
    createdAt: new Date('2024-01-14T14:30:00'),
    documentCount: 456
  },
  {
    id: '3',
    type: 'export',
    name: 'Q4_Reports.csv',
    size: 5678901,
    status: 'completed',
    createdAt: new Date('2024-01-13T09:15:00'),
    documentCount: 25
  }
])

const historyHeaders = ref<Array<{ title: string; key: string; sortable: boolean; align?: 'start' | 'end' | 'center' }>>([
  { title: 'Type', key: 'type', sortable: true },
  { title: 'Name', key: 'name', sortable: true },
  { title: 'Size', key: 'size', sortable: true },
  { title: 'Documents', key: 'documentCount', sortable: true },
  { title: 'Status', key: 'status', sortable: true },
  { title: 'Date', key: 'createdAt', sortable: true },
  { title: 'Actions', key: 'actions', sortable: false, align: 'end' }
])

// Import Methods
const handleDrop = (e: DragEvent) => {
  e.preventDefault()
  isDragging.value = false
  
  if (e.dataTransfer?.files) {
    handleFiles(e.dataTransfer.files)
  }
}

const handleFileSelect = (e: Event) => {
  const target = e.target as HTMLInputElement
  if (target.files) {
    handleFiles(target.files)
  }
}

const handleFiles = (files: FileList) => {
  for (let i = 0; i < files.length; i++) {
    selectedFiles.value.push(files[i])
  }
}

const removeFile = (index: number) => {
  selectedFiles.value.splice(index, 1)
}

const performImport = async () => {
  importing.value = true
  importProgress.value = 0
  
  // Simulate import progress
  const interval = setInterval(() => {
    importProgress.value += 10
    if (importProgress.value >= 100) {
      clearInterval(interval)
      completeImport()
    }
  }, 200)
}

const completeImport = () => {
  importing.value = false
  selectedFiles.value = []
  showNotification(`Successfully imported ${selectedFiles.value.length} file(s)`, 'success')
}

// Export Methods
const previewExport = () => {
  // TODO: Generate export preview
  exportPreview.value = {
    documentCount: 123,
    totalSize: 45678901
  }
}

const performExport = async () => {
  exporting.value = true
  exportProgress.value = 0
  
  // Simulate export progress
  const interval = setInterval(() => {
    exportProgress.value += 10
    if (exportProgress.value >= 100) {
      clearInterval(interval)
      completeExport()
    }
  }, 200)
}

const completeExport = () => {
  exporting.value = false
  showNotification('Export completed successfully', 'success')
}

const downloadExport = (item: any) => {
  // TODO: Download exported file
  showNotification(`Downloading ${item.name}`, 'info')
}

const viewDetails = (item: any) => {
  // TODO: Show details dialog
  showNotification(`Viewing details for ${item.name}`, 'info')
}

// Utility functions
const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}

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
  return status === 'completed' ? 'success' : status === 'failed' ? 'error' : 'warning'
}
</script>

<style scoped>
.import-drop-zone {
  border: 2px dashed rgba(var(--v-theme-primary), 0.5);
  border-radius: 8px;
  padding: 32px;
  text-align: center;
  cursor: pointer;
  transition: all 0.3s;
}

.import-drop-zone:hover {
  border-color: rgba(var(--v-theme-primary), 0.8);
  background-color: rgba(var(--v-theme-primary), 0.05);
}

.import-drop-zone-active {
  border-color: rgb(var(--v-theme-primary));
  background-color: rgba(var(--v-theme-primary), 0.1);
}
</style>