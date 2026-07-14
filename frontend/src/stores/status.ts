import { defineStore } from 'pinia'
import { ref } from 'vue'
import { rpcCall } from '@/api/rpc'
import type { StatusResult } from '@/api/types'

export const useStatusStore = defineStore('status', () => {
  const id = ref('')
  const peerCount = ref(0)
  const loading = ref(false)
  const error = ref<string | null>(null)

  async function refresh() {
    loading.value = true
    error.value = null
    try {
      const res = await rpcCall<StatusResult>('status')
      id.value = res.id
      peerCount.value = res.peers
    } catch (e) {
      error.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
  }

  return { id, peerCount, loading, error, refresh }
})
