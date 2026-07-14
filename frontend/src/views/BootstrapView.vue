<script setup lang="ts">
import { ref } from 'vue'
import { usePeersStore } from '@/stores/peers.ts'
import type { Contact } from '@/api/types.ts'

const peersStore = usePeersStore()

interface Row {
  id: string
  addr: string
}

const rows = ref<Row[]>([{ id: '', addr: '' }])
const submitting = ref(false)
const result = ref<'ok' | 'unreachable' | null>(null)
const error = ref<string | null>(null)

function addRow() {
  rows.value.push({ id: '', addr: '' })
}

function removeRow(index: number) {
  rows.value.splice(index, 1)
  if (rows.value.length === 0) addRow()
}

async function submit() {
  const contacts: Contact[] = rows.value
    .filter((r) => r.id.trim() && r.addr.trim())
    .map((r) => ({ id: r.id.trim(), addr: r.addr.trim() }))

  if (contacts.length === 0) {
    error.value = 'Enter at least one node ID and address.'
    return
  }

  submitting.value = true
  error.value = null
  result.value = null
  try {
    const ok = await peersStore.bootstrap(contacts)
    result.value = ok ? 'ok' : 'unreachable'
    if (ok) rows.value = [{ id: '', addr: '' }]
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e)
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div>
    <h1 class="text-h5 mb-4">Bootstrap</h1>

    <v-card variant="outlined" class="mb-4">
      <v-card-text>
        <p class="text-body-2 text-medium-emphasis mb-4">
          Connect to the network by adding one or more nodes you already know
          about. You need their node ID and QUIC address (the
          <span class="font-mono">-addr</span> the other node was started
          with, e.g. <span class="font-mono">127.0.0.1:9000</span> - not its
          JSON-RPC address).
        </p>

        <div v-for="(row, i) in rows" :key="i" class="d-flex align-center mb-2" style="gap: 8px">
          <v-text-field v-model="row.id" label="Node ID" density="compact" hide-details class="font-mono" />
          <v-text-field v-model="row.addr" label="Address" density="compact" hide-details style="max-width: 220px" />
          <v-btn icon="mdi-close" size="small" variant="text" @click="removeRow(i)" />
        </div>

        <v-btn variant="text" prepend-icon="mdi-plus" size="small" @click="addRow">Add another</v-btn>

        <v-alert v-if="error" type="error" variant="tonal" class="mt-4">{{ error }}</v-alert>
        <v-alert v-if="result === 'ok'" type="success" variant="tonal" class="mt-4">
          Connected successfully.
        </v-alert>
        <v-alert v-if="result === 'unreachable'" type="warning" variant="tonal" class="mt-4">
          None of the given nodes responded. Double-check the address and that it's reachable.
        </v-alert>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn color="primary" variant="flat" :loading="submitting" @click="submit">Bootstrap</v-btn>
      </v-card-actions>
    </v-card>
  </div>
</template>

<style scoped>
.font-mono :deep(input) {
  font-family: 'Roboto Mono', ui-monospace, monospace;
  font-size: 0.85em;
}
</style>
