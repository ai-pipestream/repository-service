<template>
  <div>
    <!-- Header -->
    <v-row class="mb-4">
      <v-col>
        <h1 class="text-h4 mb-2">
          <v-icon icon="mdi-cloud-check" class="mr-2"></v-icon>
          Discovered Services
        </h1>
        <p class="text-body-1 text-medium-emphasis">
          Services discovered through the platform registration service
        </p>
      </v-col>
      <v-col cols="auto">
        <v-btn
          color="primary"
          prepend-icon="mdi-refresh"
          @click="$emit('refresh')"
          :loading="loading"
        >
          Refresh Services
        </v-btn>
      </v-col>
    </v-row>

    <!-- Service Statistics Cards -->
    <v-row class="mb-4">
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="text-h3 text-primary">{{ discoveredServices.length }}</div>
            <div class="text-body-2 text-medium-emphasis">Total Services</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="text-h3 text-success">{{ healthyServices.length }}</div>
            <div class="text-body-2 text-medium-emphasis">Healthy Services</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="text-h3 text-warning">{{ processingModules.length }}</div>
            <div class="text-body-2 text-medium-emphasis">Processing Modules</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="3">
        <v-card>
          <v-card-text>
            <div class="text-h3 text-info">{{ grpcServices.length }}</div>
            <div class="text-body-2 text-medium-emphasis">gRPC Services</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <!-- Services Table -->
    <v-card>
      <v-card-title>
        <v-icon icon="mdi-format-list-bulleted" class="mr-2"></v-icon>
        Service Registry
        <v-spacer></v-spacer>
        <v-text-field
          v-model="search"
          density="compact"
          label="Search services..."
          prepend-inner-icon="mdi-magnify"
          variant="outlined"
          hide-details
          single-line
          style="max-width: 300px"
        ></v-text-field>
      </v-card-title>
      <v-card-text>
        <v-data-table
          :headers="headers"
          :items="discoveredServices"
          :search="search"
          :loading="loading"
          density="comfortable"
        >
          <template v-slot:item.status="{ item }">
            <v-chip
              :color="getStatusColor(item.status)"
              size="small"
              :prepend-icon="getStatusIcon(item.status)"
            >
              {{ item.status }}
            </v-chip>
          </template>
          
          <template v-slot:item.type="{ item }">
            <v-chip
              :color="getTypeColor(item.type)"
              size="small"
              variant="outlined"
            >
              {{ item.type }}
            </v-chip>
          </template>

          <template v-slot:item.registeredAt="{ item }">
            <span v-if="item.registeredAt">
              {{ formatDate(item.registeredAt) }}
            </span>
            <span v-else class="text-medium-emphasis">-</span>
          </template>

          <template v-slot:item.lastHealthCheck="{ item }">
            <span v-if="item.lastHealthCheck">
              {{ formatDate(item.lastHealthCheck) }}
            </span>
            <span v-else class="text-medium-emphasis">-</span>
          </template>

          <template v-slot:item.actions="{ item }">
            <v-btn
              icon="mdi-open-in-new"
              size="small"
              variant="text"
              :href="getServiceUrl(item.name)"
              target="_blank"
              :disabled="item.status !== 'HEALTHY'"
            ></v-btn>
            <v-btn
              icon="mdi-information"
              size="small"
              variant="text"
              @click="showServiceDetails(item)"
            ></v-btn>
          </template>

          <template v-slot:no-data>
            <div class="text-center py-8">
              <v-icon icon="mdi-cloud-off" size="64" class="text-medium-emphasis mb-4"></v-icon>
              <div class="text-h6 text-medium-emphasis">No services discovered</div>
              <div class="text-body-2 text-medium-emphasis">
                Services will appear here when they register with the platform
              </div>
            </div>
          </template>
        </v-data-table>
      </v-card-text>
    </v-card>

    <!-- Service Details Dialog -->
    <v-dialog v-model="detailsDialog" max-width="600">
      <v-card v-if="selectedService">
        <v-card-title>
          <v-icon icon="mdi-information" class="mr-2"></v-icon>
          Service Details: {{ selectedService.name }}
        </v-card-title>
        <v-card-text>
          <v-list>
            <v-list-item>
              <v-list-item-title>Service ID</v-list-item-title>
              <v-list-item-subtitle>{{ selectedService.id }}</v-list-item-subtitle>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Host</v-list-item-title>
              <v-list-item-subtitle>{{ selectedService.host }}</v-list-item-subtitle>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Port</v-list-item-title>
              <v-list-item-subtitle>{{ selectedService.port }}</v-list-item-subtitle>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Type</v-list-item-title>
              <v-list-item-subtitle>{{ selectedService.type }}</v-list-item-subtitle>
            </v-list-item>
            <v-list-item>
              <v-list-item-title>Status</v-list-item-title>
              <v-list-item-subtitle>
                <v-chip :color="getStatusColor(selectedService.status)" size="small">
                  {{ selectedService.status }}
                </v-chip>
              </v-list-item-subtitle>
            </v-list-item>
            <v-list-item v-if="selectedService.metadata && Object.keys(selectedService.metadata).length > 0">
              <v-list-item-title>Metadata</v-list-item-title>
              <v-list-item-subtitle>
                <pre class="text-caption">{{ JSON.stringify(selectedService.metadata, null, 2) }}</pre>
              </v-list-item-subtitle>
            </v-list-item>
          </v-list>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn @click="detailsDialog = false">Close</v-btn>
          <v-btn
            color="primary"
            :href="getServiceUrl(selectedService.name)"
            target="_blank"
            :disabled="selectedService.status !== 'HEALTHY'"
          >
            Open Service
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { serviceDiscovery, type DiscoveredService } from '../services/serviceDiscovery'

// Props
const props = defineProps<{
  discoveredServices: DiscoveredService[]
}>()

// Emits (not used directly in this component currently)
defineEmits<{ refresh: [] }>()

// Data
const search = ref('')
const loading = ref(false)
const detailsDialog = ref(false)
const selectedService = ref<DiscoveredService | null>(null)

// Table headers
const headers = [
  { title: 'Service Name', key: 'name', sortable: true },
  { title: 'Host', key: 'host', sortable: true },
  { title: 'Port', key: 'port', sortable: true },
  { title: 'Type', key: 'type', sortable: true },
  { title: 'Status', key: 'status', sortable: true },
  { title: 'Registered', key: 'registeredAt', sortable: true },
  { title: 'Last Health Check', key: 'lastHealthCheck', sortable: true },
  { title: 'Actions', key: 'actions', sortable: false }
]

// Computed properties
const healthyServices = computed(() => 
  props.discoveredServices.filter(service => service.status === 'HEALTHY')
)

const processingModules = computed(() => 
  props.discoveredServices.filter(service => 
    service.name.includes('chunker') || 
    service.name.includes('embedder') || 
    service.name.includes('parser') ||
    service.name.includes('sink')
  )
)

const grpcServices = computed(() => 
  props.discoveredServices.filter(service => service.type === 'GRPC')
)

// Methods
const getStatusColor = (status: string) => {
  switch (status) {
    case 'HEALTHY': return 'success'
    case 'UNHEALTHY': return 'error'
    default: return 'warning'
  }
}

const getStatusIcon = (status: string) => {
  switch (status) {
    case 'HEALTHY': return 'mdi-check-circle'
    case 'UNHEALTHY': return 'mdi-alert-circle'
    default: return 'mdi-help-circle'
  }
}

const getTypeColor = (type: string) => {
  switch (type) {
    case 'GRPC': return 'primary'
    case 'HTTP': return 'info'
    case 'WEBSOCKET': return 'warning'
    default: return 'grey'
  }
}

const formatDate = (dateString: string) => {
  return new Date(dateString).toLocaleString()
}

const getServiceUrl = (serviceName: string) => {
  return serviceDiscovery.getServiceUrl(serviceName)
}

const showServiceDetails = (service: DiscoveredService) => {
  selectedService.value = service
  detailsDialog.value = true
}
</script>
