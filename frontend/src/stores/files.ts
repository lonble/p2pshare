import { defineStore } from 'pinia'
import { ref } from 'vue'
import { rpcCall } from '@/api/rpc'
import { useCatalogStore } from './catalog.ts'
import type { DownloadResult, ListFilesResult, ListFilesResultEntry, PublishResult } from '@/api/types'

export type TaskKind = 'publish' | 'download'
export type TaskStatus = 'running' | 'done' | 'error'

export interface Task {
  id: string
  kind: TaskKind
  label: string
  done: number
  total: number
  status: TaskStatus
  error?: string
}

export const useFilesStore = defineStore('files', () => {
  const catalog = useCatalogStore()
  const files = ref<ListFilesResultEntry[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)
  const tasks = ref<Task[]>([])

  async function refresh() {
    loading.value = true
    error.value = null
    try {
      files.value = await rpcCall<ListFilesResult>('listFiles')
      for (const f of files.value) catalog.remember(f.id, f.name)
    } catch (e) {
      error.value = e instanceof Error ? e.message : String(e)
    } finally {
      loading.value = false
    }
  }

  function addTask(kind: TaskKind, label: string): Task {
    const task: Task = { id: crypto.randomUUID(), kind, label, done: 0, total: 0, status: 'running' }
    tasks.value.unshift(task)
    return task
  }

  function dismissTask(id: string) {
    tasks.value = tasks.value.filter((t) => t.id !== id)
  }

  async function publish(path: string): Promise<PublishResult> {
    const label = path.split(/[/\\]/).pop() || path
    const task = addTask('publish', label)
    try {
      const result = await rpcCall<PublishResult>('publish', { path })
      task.status = 'done'
      catalog.remember(result.id, result.manifest.name)
      await refresh()
      return result
    } catch (e) {
      task.status = 'error'
      task.error = e instanceof Error ? e.message : String(e)
      throw e
    }
  }

  async function publishUpload(name: string, dataBase64: string): Promise<PublishResult> {
    const task = addTask('publish', name)
    try {
      const result = await rpcCall<PublishResult>('publishFrontend', { name, data: dataBase64 })
      console.log(result)
      task.status = 'done'
      catalog.remember(result.id, result.manifest.name)
      await refresh()
      return result
    } catch (e) {
      task.status = 'error'
      task.error = e instanceof Error ? e.message : String(e)
      throw e
    }
  }

  async function download(id: string, outdir: string): Promise<DownloadResult> {
    const task = addTask('download', catalog.nameFor(id) ?? `${id.slice(0, 12)}…`)
    try {
      const result = await rpcCall<DownloadResult>('download', { id, outdir })
      task.status = 'done'
      await refresh()
      return result
    } catch (e) {
      task.status = 'error'
      task.error = e instanceof Error ? e.message : String(e)
      throw e
    }
  }

  return { files, loading, error, tasks, refresh, publish, publishUpload, download, dismissTask }
})
