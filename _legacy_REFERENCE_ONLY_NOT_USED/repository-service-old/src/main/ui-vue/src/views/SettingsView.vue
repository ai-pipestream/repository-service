<template>
  <div>
    <h1 class="text-h4 mb-6">Settings</h1>

    <v-tabs v-model="activeTab">
      <v-tab value="general">General</v-tab>
      <v-tab value="storage">Storage</v-tab>
      <v-tab value="search">Search</v-tab>
      <v-tab value="modules">Modules</v-tab>
      <v-tab value="security">Security</v-tab>
    </v-tabs>

    <v-window v-model="activeTab" class="mt-6">
      <!-- General Settings -->
      <v-window-item value="general">
        <v-card>
          <v-card-title>General Settings</v-card-title>
          <v-card-text>
            <v-form>
              <v-text-field
                v-model="settings.general.repositoryName"
                label="Repository Name"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-textarea
                v-model="settings.general.description"
                label="Description"
                variant="outlined"
                density="compact"
                rows="3"
                class="mb-4"
              ></v-textarea>

              <v-select
                v-model="settings.general.defaultView"
                label="Default View"
                :items="['Dashboard', 'Documents', 'Search']"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-select>

              <v-select
                v-model="settings.general.language"
                label="Language"
                :items="['English', 'Spanish', 'French', 'German']"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-select>

              <v-checkbox
                v-model="settings.general.autoSave"
                label="Enable auto-save"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.general.notifications"
                label="Enable notifications"
                density="compact"
              ></v-checkbox>
            </v-form>
          </v-card-text>
          <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn variant="text" @click="resetGeneralSettings">Reset</v-btn>
            <v-btn color="primary" variant="flat" @click="saveGeneralSettings">Save</v-btn>
          </v-card-actions>
        </v-card>
      </v-window-item>

      <!-- Storage Settings -->
      <v-window-item value="storage">
        <v-card>
          <v-card-title>Storage Configuration</v-card-title>
          <v-card-text>
            <!-- S3 Configuration -->
            <div class="text-h6 mb-4">S3 Configuration</div>
            <v-form>
              <v-text-field
                v-model="settings.storage.s3.bucket"
                label="S3 Bucket"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.storage.s3.region"
                label="AWS Region"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.storage.s3.prefix"
                label="Key Prefix"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-checkbox
                v-model="settings.storage.s3.versioning"
                label="Enable versioning"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.storage.s3.encryption"
                label="Enable server-side encryption"
                density="compact"
              ></v-checkbox>
            </v-form>

            <v-divider class="my-6"></v-divider>

            <!-- Database Configuration -->
            <div class="text-h6 mb-4">Database Configuration</div>
            <v-form>
              <v-text-field
                v-model="settings.storage.database.host"
                label="Database Host"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.storage.database.port"
                label="Port"
                variant="outlined"
                density="compact"
                type="number"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.storage.database.name"
                label="Database Name"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.storage.database.poolSize"
                label="Connection Pool Size"
                variant="outlined"
                density="compact"
                type="number"
                class="mb-4"
              ></v-text-field>
            </v-form>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="outlined" @click="testStorageConnection">Test Connection</v-btn>
            <v-spacer></v-spacer>
            <v-btn variant="text" @click="resetStorageSettings">Reset</v-btn>
            <v-btn color="primary" variant="flat" @click="saveStorageSettings">Save</v-btn>
          </v-card-actions>
        </v-card>
      </v-window-item>

      <!-- Search Settings -->
      <v-window-item value="search">
        <v-card>
          <v-card-title>OpenSearch Configuration</v-card-title>
          <v-card-text>
            <v-form>
              <v-text-field
                v-model="settings.search.endpoint"
                label="OpenSearch Endpoint"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.search.indexName"
                label="Index Name"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.search.shards"
                label="Number of Shards"
                variant="outlined"
                density="compact"
                type="number"
                class="mb-4"
              ></v-text-field>

              <v-text-field
                v-model="settings.search.replicas"
                label="Number of Replicas"
                variant="outlined"
                density="compact"
                type="number"
                class="mb-4"
              ></v-text-field>

              <v-slider
                v-model="settings.search.maxResults"
                label="Max Results per Page"
                :min="10"
                :max="100"
                :step="10"
                thumb-label="always"
                class="mb-4"
              ></v-slider>

              <v-checkbox
                v-model="settings.search.fuzzySearch"
                label="Enable fuzzy search"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.search.synonyms"
                label="Enable synonyms"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.search.highlighting"
                label="Enable result highlighting"
                density="compact"
              ></v-checkbox>
            </v-form>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="outlined" @click="reindexDocuments">Reindex All</v-btn>
            <v-spacer></v-spacer>
            <v-btn variant="text" @click="resetSearchSettings">Reset</v-btn>
            <v-btn color="primary" variant="flat" @click="saveSearchSettings">Save</v-btn>
          </v-card-actions>
        </v-card>
      </v-window-item>

      <!-- Modules Settings -->
      <v-window-item value="modules">
        <v-card>
          <v-card-title>Processing Modules</v-card-title>
          <v-card-text>
            <v-data-table
              :headers="moduleHeaders"
              :items="availableModules"
              :items-per-page="10"
            >
              <template v-slot:item.enabled="{ item }">
                <v-switch
                  v-model="item.enabled"
                  density="compact"
                  hide-details
                ></v-switch>
              </template>
              <template v-slot:item.status="{ item }">
                <v-chip :color="item.status === 'active' ? 'success' : 'grey'" size="small">
                  {{ item.status }}
                </v-chip>
              </template>
              <template v-slot:item.actions="{ item }">
                <v-btn icon="mdi-cog" size="small" variant="text" @click="configureModule(item)"></v-btn>
                <v-btn icon="mdi-refresh" size="small" variant="text" @click="refreshModule(item)"></v-btn>
              </template>
            </v-data-table>

            <v-btn color="primary" class="mt-4" prepend-icon="mdi-plus">
              Register New Module
            </v-btn>
          </v-card-text>
        </v-card>
      </v-window-item>

      <!-- Security Settings -->
      <v-window-item value="security">
        <v-card>
          <v-card-title>Security Settings</v-card-title>
          <v-card-text>
            <!-- Access Control -->
            <div class="text-h6 mb-4">Access Control</div>
            <v-form>
              <v-select
                v-model="settings.security.authMode"
                label="Authentication Mode"
                :items="['Local', 'LDAP', 'OAuth', 'SAML']"
                variant="outlined"
                density="compact"
                class="mb-4"
              ></v-select>

              <v-checkbox
                v-model="settings.security.requireMFA"
                label="Require Multi-Factor Authentication"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.security.auditLogging"
                label="Enable audit logging"
                density="compact"
              ></v-checkbox>
            </v-form>

            <v-divider class="my-6"></v-divider>

            <!-- API Keys -->
            <div class="text-h6 mb-4">API Keys</div>
            <v-list>
              <v-list-item v-for="key in apiKeys" :key="key.id">
                <template v-slot:prepend>
                  <v-icon>mdi-key</v-icon>
                </template>
                <v-list-item-title>{{ key.name }}</v-list-item-title>
                <v-list-item-subtitle>Created: {{ formatDate(key.createdAt) }}</v-list-item-subtitle>
                <template v-slot:append>
                  <v-btn icon="mdi-delete" size="small" variant="text" color="error"></v-btn>
                </template>
              </v-list-item>
            </v-list>
            <v-btn variant="outlined" class="mt-2">Generate New API Key</v-btn>

            <v-divider class="my-6"></v-divider>

            <!-- Encryption -->
            <div class="text-h6 mb-4">Encryption</div>
            <v-form>
              <v-checkbox
                v-model="settings.security.encryptAtRest"
                label="Encrypt data at rest"
                density="compact"
              ></v-checkbox>

              <v-checkbox
                v-model="settings.security.encryptInTransit"
                label="Encrypt data in transit"
                density="compact"
              ></v-checkbox>

              <v-select
                v-model="settings.security.encryptionAlgorithm"
                label="Encryption Algorithm"
                :items="['AES-256', 'AES-128', 'RSA-2048']"
                variant="outlined"
                density="compact"
              ></v-select>
            </v-form>
          </v-card-text>
          <v-card-actions>
            <v-spacer></v-spacer>
            <v-btn variant="text" @click="resetSecuritySettings">Reset</v-btn>
            <v-btn color="primary" variant="flat" @click="saveSecuritySettings">Save</v-btn>
          </v-card-actions>
        </v-card>
      </v-window-item>
    </v-window>
  </div>
</template>

<script setup lang="ts">
import { ref, inject } from 'vue'

// Inject notification
const showNotification = inject('showNotification') as Function

// State
const activeTab = ref('general')

// Settings
const settings = ref({
  general: {
    repositoryName: 'Main Repository',
    description: 'Central document repository for all organizational data',
    defaultView: 'Dashboard',
    language: 'English',
    autoSave: true,
    notifications: true
  },
  storage: {
    s3: {
      bucket: 'repository-documents',
      region: 'us-east-1',
      prefix: 'documents/',
      versioning: true,
      encryption: true
    },
    database: {
      host: 'localhost',
      port: 3306,
      name: 'repository',
      poolSize: 10
    }
  },
  search: {
    endpoint: 'http://localhost:9200',
    indexName: 'documents',
    shards: 5,
    replicas: 1,
    maxResults: 50,
    fuzzySearch: true,
    synonyms: true,
    highlighting: true
  },
  security: {
    authMode: 'OAuth',
    requireMFA: false,
    auditLogging: true,
    encryptAtRest: true,
    encryptInTransit: true,
    encryptionAlgorithm: 'AES-256'
  }
})

// Module data
const availableModules = ref([
  {
    id: '1',
    name: 'PDF Extractor',
    version: '1.2.0',
    enabled: true,
    status: 'active',
    description: 'Extracts text and metadata from PDF documents'
  },
  {
    id: '2',
    name: 'NLP Analyzer',
    version: '2.0.1',
    enabled: true,
    status: 'active',
    description: 'Natural language processing and sentiment analysis'
  },
  {
    id: '3',
    name: 'OCR Processor',
    version: '1.0.5',
    enabled: false,
    status: 'inactive',
    description: 'Optical character recognition for images'
  }
])

const moduleHeaders = ref<Array<{ title: string; key: string; sortable: boolean; align?: 'start' | 'end' | 'center' }>>([
  { title: 'Name', key: 'name', sortable: true },
  { title: 'Version', key: 'version', sortable: true },
  { title: 'Enabled', key: 'enabled', sortable: false },
  { title: 'Status', key: 'status', sortable: true },
  { title: 'Description', key: 'description', sortable: false },
  { title: 'Actions', key: 'actions', sortable: false, align: 'end' }
])

// API Keys
const apiKeys = ref([
  { id: '1', name: 'Production API', createdAt: new Date('2024-01-01') },
  { id: '2', name: 'Development API', createdAt: new Date('2024-01-10') }
])

// Methods
const saveGeneralSettings = () => {
  // TODO: Save settings to backend
  showNotification('General settings saved', 'success')
}

const resetGeneralSettings = () => {
  // TODO: Reset to defaults
  showNotification('General settings reset to defaults', 'info')
}

const saveStorageSettings = () => {
  // TODO: Save settings to backend
  showNotification('Storage settings saved', 'success')
}

const resetStorageSettings = () => {
  showNotification('Storage settings reset to defaults', 'info')
}

const testStorageConnection = () => {
  // TODO: Test S3 and database connections
  showNotification('Testing storage connections...', 'info')
  setTimeout(() => {
    showNotification('All storage connections successful', 'success')
  }, 2000)
}

const saveSearchSettings = () => {
  // TODO: Save settings to backend
  showNotification('Search settings saved', 'success')
}

const resetSearchSettings = () => {
  showNotification('Search settings reset to defaults', 'info')
}

const reindexDocuments = () => {
  // TODO: Trigger reindexing
  showNotification('Reindexing all documents...', 'info')
}

const saveSecuritySettings = () => {
  // TODO: Save settings to backend
  showNotification('Security settings saved', 'success')
}

const resetSecuritySettings = () => {
  showNotification('Security settings reset to defaults', 'info')
}

const configureModule = (module: any) => {
  // TODO: Open module configuration dialog
  showNotification(`Configuring ${module.name}`, 'info')
}

const refreshModule = (module: any) => {
  // TODO: Refresh module
  showNotification(`Refreshing ${module.name}`, 'info')
}

// Utility functions
const formatDate = (date: Date): string => {
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  }).format(date)
}
</script>