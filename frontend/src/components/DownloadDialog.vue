<script setup lang="ts">
import { ref } from 'vue'
import { useFilesStore } from '@/stores/files.ts'

const open = defineModel<boolean>('open', { required: true })

const filesStore = useFilesStore()
const id = ref('')
const outdir = ref('.')
const submitting = ref(false)
const error = ref<string | null>(null)

async function submit() {
  if (!id.value.trim()) return
  submitting.value = true
  error.value = null
  try {
    await filesStore.download(id.value.trim(), outdir.value.trim() || '.')
    id.value = ''
    open.value = false
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e)
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <v-dialog v-model="open" max-width="480">
    <v-card title="Download a file">
      <v-card-text>
        <p class="text-body-2 text-medium-emphasis mb-4">
          Paste a file ID someone shared with you. It's saved into the output
          directory on the machine your node is running on.
        </p>
        <v-text-field
          v-model="id"
          label="File ID"
          placeholder="06ba16f807e192bc0cc9b000aa7ea51c37a01c01b01e228806f468b525488393"
          class="font-mono"
          autofocus
          :error-messages="error ?? undefined"
        />
        <v-text-field v-model="outdir" label="Output directory" placeholder="." @keydown.enter="submit" />
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="open = false">Cancel</v-btn>
        <v-btn color="primary" variant="flat" :loading="submitting" @click="submit">Download</v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<style scoped>
.font-mono :deep(input) {
  font-family: 'Roboto Mono', ui-monospace, monospace;
  font-size: 0.85em;
}
</style>
