<template>
  <div>
    <h1 class="text-h4 mb-6">Search Repository</h1>

    <!-- Search Bar -->
    <v-card class="mb-4">
      <v-card-text>
        <v-text-field
          v-model="searchQuery"
          label="Search documents, metadata, and content..."
          prepend-inner-icon="mdi-magnify"
          variant="outlined"
          clearable
          @keyup.enter="performSearch"
        >
          <template v-slot:append-inner>
            <v-btn
              color="primary"
              variant="flat"
              @click="performSearch"
              :loading="searching"
            >
              Search
            </v-btn>
          </template>
        </v-text-field>
      </v-card-text>
    </v-card>

    <!-- Filters and Results -->
    <v-row>
      <!-- Filters Sidebar -->
      <v-col cols="12" md="3">
        <v-card>
          <v-card-title>Filters</v-card-title>
          <v-card-text>
            <!-- Document Type -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Document Type</div>
              <v-checkbox
                v-for="type in documentTypes"
                :key="type.value"
                v-model="selectedTypes"
                :label="type.label"
                :value="type.value"
                density="compact"
              ></v-checkbox>
            </div>

            <!-- Date Range -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Date Range</div>
              <v-select
                v-model="dateRange"
                :items="dateRangeOptions"
                variant="outlined"
                density="compact"
              ></v-select>
            </div>

            <!-- Tags -->
            <div class="mb-4">
              <div class="text-subtitle-2 mb-2">Tags</div>
              <v-chip-group v-model="selectedTags" multiple column>
                <v-chip v-for="tag in availableTags" :key="tag" filter variant="outlined">
                  {{ tag }}
                </v-chip>
              </v-chip-group>
            </div>

            <v-btn block variant="outlined" @click="resetFilters">
              Reset Filters
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>

      <!-- Search Results -->
      <v-col cols="12" md="9">
        <!-- Results Header -->
        <div class="d-flex align-center mb-4">
          <div class="text-h6">
            {{ searching ? 'Searching...' : `${searchResults.length} results found` }}
          </div>
          <v-spacer></v-spacer>
          <v-select
            v-model="sortBy"
            :items="sortOptions"
            label="Sort by"
            variant="outlined"
            density="compact"
            hide-details
            style="max-width: 200px"
          ></v-select>
        </div>

        <!-- Loading State -->
        <v-progress-linear v-if="searching" indeterminate color="primary" class="mb-4"></v-progress-linear>

        <v-alert v-if="error" type="error" class="mb-4">{{ error }}</v-alert>

        <!-- No Results -->
        <v-card v-if="!searching && searchResults.length === 0 && hasSearched">
          <v-card-text class="text-center pa-8">
            <v-icon size="64" color="grey">mdi-magnify</v-icon>
            <div class="text-h6 mt-4">No results found</div>
            <div class="text-caption">Try adjusting your search terms or filters</div>
          </v-card-text>
        </v-card>

        <!-- Results List -->
        <div v-else>
          <v-card v-for="result in searchResults" :key="result.id" class="mb-3">
            <v-card-text>
              <div class="d-flex align-center">
                <v-icon class="mr-2" :color="getFileColor(result.type)">
                  {{ getFileIcon(result.type) }}
                </v-icon>
                <div class="flex-grow-1">
                  <div class="text-h6">{{ highlightText(result.title) }}</div>
                  <div class="text-caption text-grey">
                    {{ result.path }} • {{ formatBytes(result.size) }} • {{ formatDate(result.createdAt) }}
                  </div>
                </div>
              </div>

              <!-- Tags -->
              <div v-if="result.tags && result.tags.length > 0" class="mt-2">
                <v-chip v-for="tag in result.tags" :key="tag" size="small" class="mr-1">
                  {{ tag }}
                </v-chip>
              </div>

              <!-- Actions -->
              <div class="mt-3">
                <v-btn size="small" variant="text" @click="viewDocument(result)">
                  View
                </v-btn>
                <v-btn size="small" variant="text" @click="downloadDocument(result)">
                  Download
                </v-btn>
                <v-btn size="small" variant="text" @click="addToCollection(result)">
                  Add to Collection
                </v-btn>
              </div>
            </v-card-text>
          </v-card>

          <!-- Pagination -->
          <v-pagination
            v-if="totalPages > 1"
            v-model="currentPage"
            :length="totalPages"
            :total-visible="7"
            @update:model-value="performSearch"
          ></v-pagination>
        </div>
      </v-col>
    </v-row>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useDocumentStore } from '../stores/documentStore'
import { createClient } from '@connectrpc/connect'
import { createBinaryTransport, create, SearchPipeDocsRequestSchema } from '@pipeline/proto-stubs'
import { PipeDocRepositoryService } from '@pipeline/proto-stubs'

const props = defineProps<{
  initialQuery?: string
}>()

// State Management
const documentStore = useDocumentStore()
const { documents: searchResults, loading: searching, error } = storeToRefs(documentStore)

// gRPC Client
const transport = createBinaryTransport()
const repoClient = createClient(PipeDocRepositoryService, transport)

// Local State
const searchQuery = ref(props.initialQuery || '')
const hasSearched = ref(false)
const currentPage = ref(1)
const itemsPerPage = ref(10)
const totalItems = computed(() => searchResults.value.length)

// Filters
const selectedTypes = ref<string[]>([])
const dateRange = ref('all')
const selectedTags = ref<string[]>([])
const sortBy = ref('relevance')

const documentTypes = ref([
  { label: 'PDF', value: 'pdf' },
  { label: 'Word Document', value: 'docx' },
  { label: 'Spreadsheet', value: 'xlsx' },
  { label: 'Text File', value: 'txt' },
  { label: 'Image', value: 'image' }
])

const dateRangeOptions = ref([
  { title: 'All Time', value: 'all' },
  { title: 'Today', value: 'today' },
  { title: 'This Week', value: 'week' },
  { title: 'This Month', value: 'month' },
  { title: 'This Year', value: 'year' }
])

const availableTags = ref(['financial', 'report', 'product', 'data', 'analysis', '2023', '2024'])

const sortOptions = ref([
  { title: 'Relevance', value: 'relevance' },
  { title: 'Date (Newest)', value: 'date_desc' },
  { title: 'Date (Oldest)', value: 'date_asc' },
])

// Computed
const totalPages = computed(() => Math.ceil(totalItems.value / itemsPerPage.value))

// Methods
async function performSearch() {
  if (!searchQuery.value.trim() && selectedTypes.value.length === 0 && selectedTags.value.length === 0) {
    return
  }

  hasSearched.value = true
  documentStore.setLoading(true)
  documentStore.setError(null)

  // Construct the metadata filters from the UI controls
  const metadataFilters: { [key: string]: string } = {}
  if (selectedTypes.value.length > 0) {
    metadataFilters.doc_type = selectedTypes.value.join(',')
  }
  if (dateRange.value !== 'all') {
    metadataFilters.date_range = dateRange.value
  }
  if (selectedTags.value.length > 0) {
    metadataFilters.tags = selectedTags.value.join(',')
  }

  try {
    const request = create(SearchPipeDocsRequestSchema, {
      query: searchQuery.value,
      metadataFilters: metadataFilters,
      pagination: { pageSize: itemsPerPage.value, pageNumber: currentPage.value },
      // TODO: Add sorting
    });

    console.log('Sending SearchPipeDocsRequest:', request);

    // As per our plan, the backend will log this request and return a mocked response.
    const resp = await repoClient.searchPipeDocs(request, {
      headers: { 'x-target-backend': 'repository-service' }
    });

    const mappedDocs = resp.results.map(it => ({
      id: it.storageId,
      title: it.title || 'Untitled',
      path: `id: ${it.storageId}`,
      type: it.title.split('.').pop() || 'unknown',
      size: 0, // Not in search result
      createdAt: it.createdAt?.toDate() || new Date(),
      tags: Object.values(it.tags),
      description: it.description,
      score: it.score,
      highlights: Object.values(it.highlights),
    }))
    documentStore.setDocuments(mappedDocs)

  } catch (e: any) {
    documentStore.setError(e?.message || String(e))
    documentStore.setDocuments([])
  } finally {
    documentStore.setLoading(false)
  }
}

const resetFilters = () => {
  selectedTypes.value = []
  dateRange.value = 'all'
  selectedTags.value = []
  performSearch()
}

const viewDocument = (doc: any) => { /* TODO */ }
const downloadDocument = (doc: any) => { /* TODO */ }
const addToCollection = (doc: any) => { /* TODO */ }

const highlightText = (text: string): string => {
  if (!searchQuery.value || !text) return text
  const regex = new RegExp(`(${searchQuery.value})`, 'gi')
  return text.replace(regex, '<mark>$1</mark>')
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
    day: 'numeric'
  }).format(date)
}

const getFileIcon = (type: string): string => {
  const icons: Record<string, string> = {
    pdf: 'mdi-file-pdf-box',
    docx: 'mdi-file-word',
    xlsx: 'mdi-file-excel',
    txt: 'mdi-file-document',
    image: 'mdi-file-image'
  }
  return icons[type] || 'mdi-file'
}

const getFileColor = (type: string): string => {
  const colors: Record<string, string> = {
    pdf: 'red',
    docx: 'blue',
    xlsx: 'green',
    txt: 'grey',
    image: 'orange'
  }
  return colors[type] || 'grey'
}

watch(() => props.initialQuery, (newVal) => {
  searchQuery.value = newVal || ''
  if (newVal) {
    performSearch()
  }
})
</script>

<style scoped>
.highlight-snippet {
  padding: 8px;
  background-color: rgba(var(--v-theme-primary), 0.05);
  border-left: 3px solid rgb(var(--v-theme-primary));
  margin-bottom: 8px;
  font-size: 14px;
  line-height: 1.5;
}

:deep(mark) {
  background-color: rgba(var(--v-theme-warning), 0.3);
  padding: 0 2px;
  font-weight: 500;
}
</style>
