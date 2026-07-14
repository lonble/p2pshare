import { defineStore } from 'pinia'
import { ref } from 'vue'
import { rpcCall } from '@/api/rpc.ts'
import type { BootstrapResult, Contact, PeersResult } from '@/api/types.ts'

export const usePeersStore = defineStore('peers', () => {
  const peers = ref<Contact[]>([])
  const loading = ref(false)
  const bootstrapping = ref(false)
  const error = ref<string | null>(null)

  async function refresh() {
    loading.value = true
    error.value = null
    try {
      peers.value = await rpcCall<PeersResult>('peers')
    } catch (e) {
      error.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
  }

  /** Adds one or more bootstrap contacts. Returns true if at least one responded. */
  async function bootstrap(contacts: Contact[]): Promise<boolean> {
    bootstrapping.value = true
    error.value = null
    try {
      const res = await rpcCall<BootstrapResult>('bootstrap', contacts)
      await refresh()
      return res.ok
    } catch (e) {
      error.value = e instanceof Error ? e.message : String(e)
      throw e
    } finally {
      bootstrapping.value = false
    }
  }

  return { peers, loading, bootstrapping, error, refresh, bootstrap }
})
