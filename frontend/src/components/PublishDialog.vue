<script setup lang="ts">
import { ref, watch } from 'vue'
import { useFilesStore } from '@/stores/files.ts'

const open = defineModel<boolean>('open', { required: true })

const filesStore = useFilesStore()
const file = ref<File | null>(null)
const name = ref('')
const submitting = ref(false)
const error = ref<string | null>(null)

const MAX_UPLOAD_BYTES = 500 * 1024 * 1024

watch(file, (f) => {
  if (f) name.value = f.name
})

function readAsBase64(f: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = reader.result as string
      resolve(result.slice(result.indexOf(',') + 1))
    }
    reader.onerror = () => reject(reader.error ?? new Error('Datei konnte nicht gelesen werden'))
    reader.readAsDataURL(f)
  })
}

async function submit() {
  if (!file.value) return
  if (!name.value.trim()) {
    error.value = 'Please enter a name for the file'
    return
  }
  if (file.value.size > MAX_UPLOAD_BYTES) {
    error.value = 'File is larger than 500 MB and cannot be processed.'
    return
  }
  submitting.value = true
  error.value = null
  try {
    const data = await readAsBase64(file.value)
    await filesStore.publishUpload(name.value.trim(), data)
    file.value = null
    name.value = ''
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
    <v-card title="Publish a file">
      <v-card-text>
        <p class="text-body-2 text-medium-emphasis mb-4">
          Upload a file from your computer. The file must be smaller than 500 MB.
        </p>
        <v-file-input
          v-model="file"
          label="File"
          prepend-icon="mdi-file-upload-outline"
          autofocus
          show-size
          clearable
        />
        <v-text-field
          v-model="name"
          label="Publish as"
          placeholder="report.pdf"
          :error-messages="error ?? undefined"
          @keydown.enter="submit"
        />
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="open = false">Cancel</v-btn>
        <v-btn color="primary" variant="flat" :loading="submitting" :disabled="!file" @click="submit">
          Publish
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>
