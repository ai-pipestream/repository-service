import { defineStore } from 'pinia'
import { ref } from 'vue'

// TODO: Define a proper type for the document based on the proto definitions
type Document = any;

export const useDocumentStore = defineStore('documents', () => {
  const documents = ref<Document[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  function setDocuments(newDocuments: Document[]) {
    documents.value = newDocuments
  }

  function setLoading(isLoading: boolean) {
    loading.value = isLoading
  }

  function setError(errorMessage: string | null) {
    error.value = errorMessage
  }

  return {
    documents,
    loading,
    error,
    setDocuments,
    setLoading,
    setError,
  }
})
