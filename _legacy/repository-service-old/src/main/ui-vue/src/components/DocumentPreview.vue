<template>
  <v-card>
    <v-card-title class="d-flex align-center">
      <v-icon class="mr-2">{{ getFileIcon(document?.type) }}</v-icon>
      {{ document?.name }}
      <v-spacer></v-spacer>
      <v-btn icon="mdi-close" variant="text" @click="$emit('close')"></v-btn>
    </v-card-title>

    <v-divider></v-divider>

    <v-card-text>
      <!-- Document Info -->
      <v-row>
        <v-col cols="12" md="8">
          <!-- Preview Area -->
          <div class="preview-container">
            <div v-if="isLoading" class="text-center pa-8">
              <v-progress-circular indeterminate color="primary"></v-progress-circular>
              <div class="mt-4">Loading document...</div>
            </div>

            <div v-else-if="error" class="text-center pa-8">
              <v-icon size="64" color="error">mdi-alert-circle</v-icon>
              <div class="text-h6 mt-4">Error loading document</div>
              <div class="text-caption">{{ error }}</div>
            </div>

            <div v-else-if="isPdf" class="pdf-preview">
              <v-icon size="64" color="primary">mdi-file-pdf-box</v-icon>
              <div class="text-h6 mt-4">PDF Document</div>
              <div class="text-caption">{{ formatBytes(document.size) }}</div>
              <v-btn color="primary" class="mt-4" prepend-icon="mdi-download">
                Download to View
              </v-btn>
            </div>

            <div v-else-if="isImage" class="image-preview">
              <img :src="previewUrl" :alt="document.name" style="max-width: 100%; height: auto;">
            </div>

            <div v-else-if="isText" class="text-preview">
              <pre class="document-content">{{ documentContent }}</pre>
            </div>

            <div v-else class="unsupported-preview text-center pa-8">
              <v-icon size="64" color="grey">{{ getFileIcon(document?.type) }}</v-icon>
              <div class="text-h6 mt-4">Preview not available</div>
              <div class="text-caption">{{ document?.type }} files cannot be previewed</div>
              <v-btn color="primary" class="mt-4" prepend-icon="mdi-download">
                Download File
              </v-btn>
            </div>
          </div>
        </v-col>

        <v-col cols="12" md="4">
          <!-- Metadata Panel -->
          <v-list>
            <v-list-subheader>Document Information</v-list-subheader>
            
            <v-list-item>
              <template v-slot:prepend>
                <v-icon>mdi-file</v-icon>
              </template>
              <v-list-item-title>Type</v-list-item-title>
              <v-list-item-subtitle>{{ document?.type?.toUpperCase() || 'Unknown' }}</v-list-item-subtitle>
            </v-list-item>

            <v-list-item>
              <template v-slot:prepend>
                <v-icon>mdi-database</v-icon>
              </template>
              <v-list-item-title>Size</v-list-item-title>
              <v-list-item-subtitle>{{ formatBytes(document?.size || 0) }}</v-list-item-subtitle>
            </v-list-item>

            <v-list-item>
              <template v-slot:prepend>
                <v-icon>mdi-calendar</v-icon>
              </template>
              <v-list-item-title>Created</v-list-item-title>
              <v-list-item-subtitle>{{ formatDate(document?.createdAt) }}</v-list-item-subtitle>
            </v-list-item>

            <v-list-item v-if="document?.modifiedAt">
              <template v-slot:prepend>
                <v-icon>mdi-calendar-edit</v-icon>
              </template>
              <v-list-item-title>Modified</v-list-item-title>
              <v-list-item-subtitle>{{ formatDate(document?.modifiedAt) }}</v-list-item-subtitle>
            </v-list-item>
          </v-list>

          <v-divider class="my-4"></v-divider>

          <!-- Description -->
          <div class="px-4">
            <div class="text-subtitle-2 mb-2">Description</div>
            <div v-if="editingDescription">
              <v-textarea
                v-model="localDescription"
                variant="outlined"
                density="compact"
                rows="3"
              ></v-textarea>
              <div class="d-flex gap-2 mt-2">
                <v-btn size="small" @click="saveDescription">Save</v-btn>
                <v-btn size="small" variant="text" @click="cancelEditDescription">Cancel</v-btn>
              </div>
            </div>
            <div v-else>
              <div class="text-body-2 text-grey-darken-1">
                {{ document?.description || 'No description' }}
              </div>
              <v-btn
                size="small"
                variant="text"
                class="mt-2"
                @click="editDescription"
              >
                Edit
              </v-btn>
            </div>
          </div>

          <v-divider class="my-4"></v-divider>

          <!-- Tags -->
          <div class="px-4">
            <div class="text-subtitle-2 mb-2">Tags</div>
            <div v-if="editingTags">
              <v-combobox
                v-model="localTags"
                variant="outlined"
                density="compact"
                multiple
                chips
                closable-chips
                :items="suggestedTags"
              ></v-combobox>
              <div class="d-flex gap-2 mt-2">
                <v-btn size="small" @click="saveTags">Save</v-btn>
                <v-btn size="small" variant="text" @click="cancelEditTags">Cancel</v-btn>
              </div>
            </div>
            <div v-else>
              <v-chip-group v-if="document?.tags?.length > 0">
                <v-chip v-for="tag in document.tags" :key="tag" size="small">
                  {{ tag }}
                </v-chip>
              </v-chip-group>
              <div v-else class="text-body-2 text-grey">No tags</div>
              <v-btn
                size="small"
                variant="text"
                class="mt-2"
                @click="editTags"
              >
                Edit Tags
              </v-btn>
            </div>
          </div>
        </v-col>
      </v-row>
    </v-card-text>

    <v-card-actions>
      <v-btn prepend-icon="mdi-download" @click="downloadDocument">
        Download
      </v-btn>
      <v-btn prepend-icon="mdi-share-variant" @click="shareDocument">
        Share
      </v-btn>
      <v-spacer></v-spacer>
      <v-btn color="error" variant="text" prepend-icon="mdi-delete" @click="confirmDelete">
        Delete
      </v-btn>
    </v-card-actions>

    <!-- Delete Confirmation Dialog -->
    <v-dialog v-model="showDeleteDialog" max-width="400">
      <v-card>
        <v-card-title>Delete Document?</v-card-title>
        <v-card-text>
          Are you sure you want to delete "{{ document?.name }}"? This action cannot be undone.
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn variant="text" @click="showDeleteDialog = false">Cancel</v-btn>
          <v-btn color="error" variant="flat" @click="deleteDocument">Delete</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-card>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, inject } from 'vue'

const props = defineProps<{
  document: any
}>()

const emit = defineEmits(['close', 'deleted', 'updated'])

// Inject notification
const showNotification = inject('showNotification') as Function

// State
const isLoading = ref(false)
const error = ref('')
const documentContent = ref('')
const previewUrl = ref('')

// Edit states
const editingDescription = ref(false)
const localDescription = ref('')
const editingTags = ref(false)
const localTags = ref<string[]>([])
const suggestedTags = ref(['document', 'report', 'data', 'archive', '2024'])

// Dialogs
const showDeleteDialog = ref(false)

// Computed properties
const isPdf = computed(() => props.document?.type === 'pdf')
const isImage = computed(() => ['jpg', 'jpeg', 'png', 'gif', 'webp'].includes(props.document?.type))
const isText = computed(() => ['txt', 'md', 'json', 'xml', 'csv', 'log'].includes(props.document?.type))

// Methods
const loadDocument = async () => {
  isLoading.value = true
  error.value = ''
  
  try {
    // TODO: Load actual document content from S3 via repository service
    if (isText.value) {
      // Simulate loading text content
      documentContent.value = `This is a preview of ${props.document.name}\n\nContent would be loaded from S3...`
    } else if (isImage.value) {
      // Simulate loading image URL
      previewUrl.value = '/placeholder-image.jpg'
    }
  } catch (e: any) {
    error.value = e.message || 'Failed to load document'
  } finally {
    isLoading.value = false
  }
}

const downloadDocument = () => {
  // TODO: Implement download from S3
  showNotification(`Downloading ${props.document.name}...`, 'info')
}

const shareDocument = () => {
  // TODO: Implement share functionality
  showNotification('Share functionality coming soon', 'info')
}

const confirmDelete = () => {
  showDeleteDialog.value = true
}

const deleteDocument = () => {
  // TODO: Implement delete from S3 and database
  showDeleteDialog.value = false
  emit('deleted', props.document)
  showNotification(`Document ${props.document.name} deleted`, 'success')
  emit('close')
}

// Description editing
const editDescription = () => {
  localDescription.value = props.document?.description || ''
  editingDescription.value = true
}

const saveDescription = () => {
  // TODO: Save to database
  const updatedDoc = { ...props.document, description: localDescription.value }
  emit('updated', updatedDoc)
  editingDescription.value = false
  showNotification('Description updated', 'success')
}

const cancelEditDescription = () => {
  editingDescription.value = false
}

// Tags editing
const editTags = () => {
  localTags.value = [...(props.document?.tags || [])]
  editingTags.value = true
}

const saveTags = () => {
  // TODO: Save to database
  const updatedDoc = { ...props.document, tags: localTags.value }
  emit('updated', updatedDoc)
  editingTags.value = false
  showNotification('Tags updated', 'success')
}

const cancelEditTags = () => {
  editingTags.value = false
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
  if (!date) return 'Unknown'
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date)
}

const getFileIcon = (type: string): string => {
  const icons: Record<string, string> = {
    pdf: 'mdi-file-pdf-box',
    docx: 'mdi-file-word',
    csv: 'mdi-file-excel',
    txt: 'mdi-file-document',
    image: 'mdi-file-image'
  }
  return icons[type] || 'mdi-file'
}

// Load document on mount
onMounted(() => {
  if (props.document) {
    loadDocument()
  }
})
</script>

<style scoped>
.preview-container {
  min-height: 400px;
  border: 1px solid rgba(0, 0, 0, 0.12);
  border-radius: 4px;
  overflow: auto;
}

.pdf-preview,
.unsupported-preview {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 400px;
}

.text-preview {
  padding: 16px;
}

.document-content {
  white-space: pre-wrap;
  word-wrap: break-word;
  font-family: monospace;
  font-size: 14px;
}

.image-preview {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  min-height: 400px;
}
</style>