<template>
  <v-card>
    <v-card-title>
      <v-icon class="mr-2">mdi-upload</v-icon>
      Upload Document
    </v-card-title>
    
    <v-card-text>
      <!-- Drag and Drop Area -->
      <div
        class="drop-zone"
        :class="{ 'drop-zone-active': isDragging }"
        @drop="handleDrop"
        @dragover.prevent
        @dragenter.prevent="isDragging = true"
        @dragleave.prevent="isDragging = false"
      >
        <v-icon size="64" color="primary">mdi-cloud-upload</v-icon>
        <div class="text-h6 mt-2">Drag and drop files here</div>
        <div class="text-caption text-grey">or click to browse</div>
        <v-btn
          color="primary"
          variant="outlined"
          class="mt-4"
          @click="onBrowseClick"
        >
          Browse Files
        </v-btn>
        <v-btn
          color="primary"
          variant="text"
          class="mt-2"
          @click="onBrowseFolderClick"
        >
          Browse Folder
        </v-btn>
        <input
          ref="fileInput"
          type="file"
          multiple
          hidden
          @change="handleFileSelect"
        />
        <!-- Non-standard but widely supported (Chromium) directory picker -->
        <input
          ref="folderInput"
          type="file"
          hidden
          multiple
          webkitdirectory
          directory
          @change="handleFolderSelect"
        />
      </div>

      <!-- Selected Files -->
      <v-list v-if="selectedFiles.length > 0" class="mt-4">
        <v-list-subheader>Selected Files</v-list-subheader>
        <v-list-item v-for="(file, index) in selectedFiles" :key="index">
          <template v-slot:prepend>
            <v-icon>{{ getFileIcon(file.type) }}</v-icon>
          </template>
          <v-list-item-title>{{ displayName(file) }}</v-list-item-title>
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

      <!-- Metadata Form -->
      <v-form v-if="selectedFiles.length > 0" class="mt-4">
        <v-text-field
          v-model="metadata.description"
          label="Description"
          variant="outlined"
          density="compact"
        ></v-text-field>
        
        <v-combobox
          v-model="metadata.tags"
          label="Tags"
          variant="outlined"
          density="compact"
          multiple
          chips
          closable-chips
          :items="suggestedTags"
        ></v-combobox>
      </v-form>

      <!-- Upload Progress -->
      <v-progress-linear
        v-if="uploading"
        :model-value="uploadProgress"
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
      <v-btn variant="text" @click="$emit('close')">Cancel</v-btn>
      <v-btn
        color="primary"
        variant="flat"
        :disabled="selectedFiles.length === 0 || uploading"
        :loading="uploading"
        @click="uploadFiles"
      >
        Upload
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import { ref, inject } from 'vue'
import { uploadMultipleFiles } from '../services/repositoryClient'
// File input ref
const fileInput = ref<HTMLInputElement | null>(null)
const folderInput = ref<HTMLInputElement | null>(null)

const onBrowseClick = () => {
  fileInput.value?.click()
}

const onBrowseFolderClick = () => {
  folderInput.value?.click()
}

const emit = defineEmits(['close', 'uploaded'])

// Inject notification
const showNotification = inject('showNotification') as Function

// State
const isDragging = ref(false)
const selectedFiles = ref<File[]>([])
const uploading = ref(false)
const uploadProgress = ref(0)

// Metadata
const metadata = ref({
  description: '',
  tags: [] as string[]
})

const suggestedTags = ref(['document', 'report', 'data', 'archive', '2024'])

// Methods
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

const handleFolderSelect = (e: Event) => {
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

const uploadFiles = async () => {
  uploading.value = true
  uploadProgress.value = 0
  
  try {
    // Use the actual repository service to upload files
    const results = await uploadMultipleFiles(
      selectedFiles.value,
      metadata.value.tags,
      metadata.value.description,
      (current, total) => {
        uploadProgress.value = Math.round((current / total) * 100)
      }
    )
    
    // Check for any errors
    const errors = results.filter(r => 'error' in r.result)
    const successful = results.filter(r => 'storageId' in r.result)
    
    if (errors.length > 0) {
      console.error('Upload errors:', errors)
      showNotification(
        `Uploaded ${successful.length} file(s) with ${errors.length} error(s)`,
        errors.length === results.length ? 'error' : 'warning'
      )
    } else {
      showNotification(`Successfully uploaded ${successful.length} file(s)`, 'success')
    }
    
    // Emit uploaded documents for successful uploads
    successful.forEach(({ file, result }) => {
      if ('storageId' in result) {
        emit('uploaded', {
          storageId: result.storageId,
          name: file.name,
          type: file.type.split('/')[1] || 'unknown',
          size: file.size,
          createdAt: new Date(),
          tags: metadata.value.tags,
          description: metadata.value.description
        })
      }
    })
    
    // Close dialog if at least one file was uploaded successfully
    if (successful.length > 0) {
      emit('close')
    }
  } catch (error: any) {
    console.error('Upload failed:', error)
    showNotification(`Upload failed: ${error.message}`, 'error')
  } finally {
    uploading.value = false
  }
}

// Utility functions
const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}

const getFileIcon = (type: string): string => {
  if (type.includes('pdf')) return 'mdi-file-pdf-box'
  if (type.includes('word')) return 'mdi-file-word'
  if (type.includes('excel') || type.includes('sheet')) return 'mdi-file-excel'
  if (type.includes('image')) return 'mdi-file-image'
  return 'mdi-file'
}

const displayName = (file: File): string => {
  const rel = (file as any).webkitRelativePath as string | undefined
  return rel && rel.length > 0 ? rel : file.name
}
</script>

<style scoped>
.drop-zone {
  border: 2px dashed rgba(var(--v-theme-primary), 0.5);
  border-radius: 8px;
  padding: 48px;
  text-align: center;
  cursor: pointer;
  transition: all 0.3s;
}

.drop-zone:hover {
  border-color: rgba(var(--v-theme-primary), 0.8);
  background-color: rgba(var(--v-theme-primary), 0.05);
}

.drop-zone-active {
  border-color: rgb(var(--v-theme-primary));
  background-color: rgba(var(--v-theme-primary), 0.1);
}
</style>