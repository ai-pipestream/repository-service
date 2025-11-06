<template>
  <div>
    <h1 class="text-h4 mb-6">Repository Dashboard</h1>
    
    <!-- Stats Cards -->
    <v-row>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="pb-0">
            <div class="text-overline mb-1">Total Documents</div>
            <div class="text-h4">{{ stats.totalDocuments }}</div>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="text" size="small" @click="navigateToDocuments">
              View All
              <v-icon end>mdi-arrow-right</v-icon>
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="pb-0">
            <div class="text-overline mb-1">Process Requests</div>
            <div class="text-h4">{{ stats.totalRequests }}</div>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="text" size="small" @click="navigateToRequests">
              View All
              <v-icon end>mdi-arrow-right</v-icon>
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="pb-0">
            <div class="text-overline mb-1">Storage Used</div>
            <div class="text-h4">{{ formatBytes(stats.storageUsed) }}</div>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="text" size="small">
              Details
              <v-icon end>mdi-information</v-icon>
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text class="pb-0">
            <div class="text-overline mb-1">System Status</div>
            <div class="d-flex align-center">
              <v-icon :color="systemStatus.color" class="mr-2">
                {{ systemStatus.icon }}
              </v-icon>
              <div class="text-h5">{{ systemStatus.text }}</div>
            </div>
          </v-card-text>
          <v-card-actions>
            <v-btn variant="text" size="small">
              Health Check
              <v-icon end>mdi-heart-pulse</v-icon>
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>
    </v-row>

    <!-- Recent Activity -->
    <v-row class="mt-4">
      <v-col cols="12" md="8">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-history</v-icon>
            Recent Activity
          </v-card-title>
          <v-card-text>
            <v-list>
              <v-list-item v-for="activity in recentActivity" :key="activity.id">
                <template v-slot:prepend>
                  <v-icon :color="activity.color">{{ activity.icon }}</v-icon>
                </template>
                <v-list-item-title>{{ activity.title }}</v-list-item-title>
                <v-list-item-subtitle>{{ activity.time }}</v-list-item-subtitle>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Quick Actions -->
      <v-col cols="12" md="4">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-lightning-bolt</v-icon>
            Quick Actions
          </v-card-title>
          <v-card-text>
            <v-btn 
              block 
              color="primary" 
              class="mb-2"
              prepend-icon="mdi-upload"
              @click="showUploadDialog = true"
            >
              Upload Document
            </v-btn>
            <v-btn 
              block 
              variant="outlined" 
              class="mb-2"
              prepend-icon="mdi-magnify"
              @click="navigateToSearch"
            >
              Search Repository
            </v-btn>
            <v-btn 
              block 
              variant="outlined"
              prepend-icon="mdi-export"
              @click="navigateToExport"
            >
              Export Data
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- System Health -->
    <v-row class="mt-4">
      <v-col cols="12">
        <v-card>
          <v-card-title>
            <v-icon class="mr-2">mdi-server</v-icon>
            System Components
          </v-card-title>
          <v-card-text>
            <v-row>
              <v-col cols="12" sm="6" md="3">
                <div class="text-center">
                  <v-icon size="48" :color="s3Status.color">mdi-aws</v-icon>
                  <div class="text-h6 mt-2">S3 Storage</div>
                  <div :class="`text-${s3Status.color}`">{{ s3Status.text }}</div>
                </div>
              </v-col>
              <v-col cols="12" sm="6" md="3">
                <div class="text-center">
                  <v-icon size="48" :color="dbStatus.color">mdi-database</v-icon>
                  <div class="text-h6 mt-2">MySQL Database</div>
                  <div :class="`text-${dbStatus.color}`">{{ dbStatus.text }}</div>
                </div>
              </v-col>
              <v-col cols="12" sm="6" md="3">
                <div class="text-center">
                  <v-icon size="48" :color="searchStatus.color">mdi-magnify-scan</v-icon>
                  <div class="text-h6 mt-2">OpenSearch</div>
                  <div :class="`text-${searchStatus.color}`">{{ searchStatus.text }}</div>
                </div>
              </v-col>
              <v-col cols="12" sm="6" md="3">
                <div class="text-center">
                  <v-icon size="48" :color="kafkaStatus.color">mdi-apache-kafka</v-icon>
                  <div class="text-h6 mt-2">Kafka</div>
                  <div :class="`text-${kafkaStatus.color}`">{{ kafkaStatus.text }}</div>
                </div>
              </v-col>
            </v-row>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

// Stats
const stats = ref({
  totalDocuments: 0,
  totalRequests: 0,
  storageUsed: 0
})

// System Status
const systemStatus = ref({
  color: 'success',
  icon: 'mdi-check-circle',
  text: 'Healthy'
})

// Component Status
const s3Status = ref({ color: 'success', text: 'Connected' })
const dbStatus = ref({ color: 'success', text: 'Connected' })
const searchStatus = ref({ color: 'warning', text: 'Indexing' })
const kafkaStatus = ref({ color: 'success', text: 'Connected' })

// Recent Activity
const recentActivity = ref([
  { id: 1, icon: 'mdi-upload', color: 'primary', title: 'Document uploaded: report.pdf', time: '2 minutes ago' },
  { id: 2, icon: 'mdi-magnify', color: 'info', title: 'Search performed: "quarterly results"', time: '5 minutes ago' },
  { id: 3, icon: 'mdi-cog', color: 'warning', title: 'Process request created', time: '10 minutes ago' },
  { id: 4, icon: 'mdi-delete', color: 'error', title: 'Document deleted: old-data.csv', time: '1 hour ago' },
])

// Dialog
const showUploadDialog = ref(false)

// Navigation helpers
const navigateToDocuments = () => {
  router.push('/documents')
}

const navigateToRequests = () => {
  // TODO: Add route for requests
  // router.push('/requests')
}

const navigateToSearch = () => {
  router.push('/search')
}

const navigateToExport = () => {
  router.push('/import-export')
}

// Utility functions
const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}

// Load data on mount
onMounted(async () => {
  // TODO: Load actual stats from repository service
  stats.value = {
    totalDocuments: 1234,
    totalRequests: 56,
    storageUsed: 5368709120 // 5GB
  }
})
</script>