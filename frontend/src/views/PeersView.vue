<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { usePeersStore } from '@/stores/peers.ts'

const peersStore = usePeersStore()
const router = useRouter()

const headers = [
  { title: 'Node ID', key: 'id' },
  { title: 'Address', key: 'addr', width: 220 },
]

let poll: ReturnType<typeof setInterval> | undefined

onMounted(() => {
  peersStore.refresh()
  poll = setInterval(() => peersStore.refresh(), 5000)
})
onUnmounted(() => {
  if (poll) clearInterval(poll)
})
</script>

<template>
  <div>
    <div class="d-flex align-center mb-4">
      <h1 class="text-h5">Peers</h1>
      <v-spacer />
      <v-btn variant="text" icon="mdi-refresh" :loading="peersStore.loading" @click="peersStore.refresh()" />
      <v-btn color="primary" variant="flat" prepend-icon="mdi-connection" class="ml-2" @click="router.push('/bootstrap')">
        Bootstrap
      </v-btn>
    </div>

    <v-alert v-if="peersStore.error" type="error" variant="tonal" class="mb-4" closable>
      {{ peersStore.error }}
    </v-alert>

    <v-card variant="outlined">
      <v-data-table
        :headers="headers"
        :items="peersStore.peers"
        :loading="peersStore.loading"
        item-value="id"
        density="comfortable"
        no-data-text="No peers connected yet - bootstrap with a known node to join the network."
      >
        <template #item.id="{ item }">
          <span class="font-mono text-caption">{{ item.id }}</span>
        </template>
      </v-data-table>
    </v-card>
  </div>
</template>

<style scoped>
.font-mono {
  font-family: 'Roboto Mono', ui-monospace, monospace;
}
</style>
