<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { useFilesStore } from '@/stores/files.ts'
import { copyToClipboard, formatBytes, truncateId } from '@/utils/format.ts'
import PublishDialog from '@/components/PublishDialog.vue'
import DownloadDialog from '@/components/DownloadDialog.vue'
// import TaskProgressList from '@/components/TaskProgressList.vue'

const filesStore = useFilesStore()

const publishOpen = ref(false)
const downloadOpen = ref(false)
const copiedId = ref<string | null>(null)

const headers = [
  { title: 'Name', key: 'name' },
  { title: 'Size', key: 'size', width: 110 },
  { title: 'Chunk size', key: 'chunk_size', width: 110 },
  { title: 'Chunks', key: 'chunks', width: 90 },
  { title: 'ID', key: 'id', width: 220 },
]

async function copyId(id: string) {
  await copyToClipboard(id)
  copiedId.value = id
  setTimeout(() => {
    if (copiedId.value === id) copiedId.value = null
  }, 1500)
}

let poll: ReturnType<typeof setInterval> | undefined

onMounted(() => {
  filesStore.refresh()
  poll = setInterval(() => filesStore.refresh(), 5000)
})
onUnmounted(() => {
  if (poll) clearInterval(poll)
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h5">Files</h1>
      <v-spacer />
      <v-btn variant="text" icon="mdi-refresh" :loading="filesStore.loading" @click="filesStore.refresh()" />
      <v-btn variant="tonal" prepend-icon="mdi-download" class="ml-2" @click="downloadOpen = true">
        Download
      </v-btn>
      <v-btn color="primary" variant="flat" prepend-icon="mdi-upload" class="ml-2" @click="publishOpen = true">
        Publish
      </v-btn>
    </div>

    <v-alert v-if="filesStore.error" type="error" variant="tonal" class="mb-4" closable>
      {{ filesStore.error }}
    </v-alert>
    <!--
    <v-card v-if="filesStore.tasks.length" class="mb-4" variant="outlined">
      <TaskProgressList :tasks="filesStore.tasks" @dismiss="filesStore.dismissTask" />
    </v-card>
  -->
    <v-card variant="outlined">
      <v-data-table
        :headers="headers"
        :items="filesStore.files"
        :loading="filesStore.loading"
        item-value="id"
        density="comfortable"
        no-data-text="No files yet - publish one to get started."
      >
        <template #item.size="{ item }">{{ formatBytes(item.size) }}</template>
        <template #item.chunk_size="{ item }">{{ formatBytes(item.chunk_size) }}</template>
        <template #item.id="{ item }">
          <div class="d-flex align-center font-mono text-caption">
            {{ truncateId(item.id) }}
            <v-btn
              :icon="copiedId === item.id ? 'mdi-check' : 'mdi-content-copy'"
              size="x-small"
              variant="text"
              class="ml-1"
              @click="copyId(item.id)"
            />
          </div>
        </template>
      </v-data-table>
    </v-card>

    <PublishDialog v-model:open="publishOpen" />
    <DownloadDialog v-model:open="downloadOpen" />
  </div>
</template>

<style scoped>
.font-mono {
  font-family: 'Roboto Mono', ui-monospace, monospace;
}
</style>
