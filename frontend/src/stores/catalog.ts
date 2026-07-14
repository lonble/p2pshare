import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

const STORAGE_KEY = 'p2pshare.catalog'

export interface CatalogEntry {
  id: string
  name: string
}

function load(): Record<string, string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? JSON.parse(raw) : {}
  } catch {
    return {}
  }
}

/**
 * Local-only address book mapping a file's content id to a human-friendly
 * name. There is no name index in the DHT itself - `listFiles` only shows
 * files this node published - so any id<->name pairing this store doesn't
 * already know (e.g. an id someone sent you out of band) has to be added
 * by hand once. From then on it's remembered on this machine.
 */
export const useCatalogStore = defineStore('catalog', () => {
  const entries = ref<Record<string, string>>(load())

  watch(
    entries,
    (val) => {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(val))
      } catch {
        // ignore quota/storage errors, non-critical
      }
    },
    { deep: true },
  )

  function remember(id: string, name: string) {
    if (!id || !name) return
    entries.value[id] = name
  }

  function forget(id: string) {
    delete entries.value[id]
  }

  function nameFor(id: string): string | undefined {
    return entries.value[id]
  }

  function idForName(name: string): string | undefined {
    const needle = name.trim().toLowerCase()
    for (const [id, n] of Object.entries(entries.value)) {
      if (n.toLowerCase() === needle) return id
    }
    return undefined
  }

  function list(): CatalogEntry[] {
    return Object.entries(entries.value)
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }

  return { entries, remember, forget, nameFor, idForName, list }
})
