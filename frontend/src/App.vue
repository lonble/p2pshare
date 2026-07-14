<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { useStatusStore } from '@/stores/status'
import { truncateId, copyToClipboard } from '@/utils/format'
import { RPC_URL, setRpcUrl } from '@/api/rpc'

const statusStore = useStatusStore()
const route = useRoute()
const copied = ref(false)

const navItems = [
  { title: 'Files', to: '/', icon: 'mdi-file-multiple-outline' },
  { title: 'Peers', to: '/peers', icon: 'mdi-lan' },
  { title: 'Bootstrap', to: '/bootstrap', icon: 'mdi-connection' },
]

const nodePresets = [1, 2, 3, 4].map((n) => ({
  title: `Node ${n} (:800${n})`,
  value: `http://localhost:800${n}/`,
}))

const rpcTarget = ref(RPC_URL)
const customUrl = ref('')

function selectPreset(url: string) {
  if (url === RPC_URL) return
  setRpcUrl(url)
}

function applyCustomUrl() {
  const url = customUrl.value.trim()
  if (url && url !== RPC_URL) setRpcUrl(url)
}

async function copyNodeId() {
  if (!statusStore.id) return
  await copyToClipboard(statusStore.id)
  copied.value = true
  setTimeout(() => (copied.value = false), 1500)
}

let poll: ReturnType<typeof setInterval> | undefined

onMounted(() => {
  statusStore.refresh()
  poll = setInterval(() => statusStore.refresh(), 5000)
})
onUnmounted(() => {
  if (poll) clearInterval(poll)
})
</script>

<template>
  <v-app>
    <v-navigation-drawer permanent theme="dark" width="240">
      <div class="pa-4">
        <span class="text-h6 font-weight-bold">p2pshare</span>
      </div>

      <v-list nav density="compact">
        <v-list-item
          v-for="item in navItems"
          :key="item.to"
          :to="item.to"
          :prepend-icon="item.icon"
          :title="item.title"
          :active="route.path === item.to"
          rounded="lg"
        />
      </v-list>

      <template #append>
        <v-divider />
        <div class="pa-3">
          <div class="text-caption text-medium-emphasis mb-1">This tab talks to</div>
          <v-select
            :model-value="rpcTarget"
            :items="[...nodePresets, { title: 'Custom…', value: '__custom__' }]"
            density="compact"
            variant="outlined"
            hide-details
            class="mb-2"
            @update:model-value="(v) => (v === '__custom__' ? null : selectPreset(v as string))"
          />
          <v-text-field
            v-model="customUrl"
            placeholder="http://localhost:8005/"
            density="compact"
            variant="outlined"
            hide-details
            class="mb-3"
            append-inner-icon="mdi-arrow-right-circle-outline"
            @click:append-inner="applyCustomUrl"
            @keydown.enter="applyCustomUrl"
          />
          <div class="d-flex align-center text-caption text-medium-emphasis mb-1">
            <v-icon :color="statusStore.peerCount > 0 ? 'success' : 'grey'" icon="mdi-circle" size="8" class="mr-2" />
            {{ statusStore.peerCount }} peer{{ statusStore.peerCount === 1 ? '' : 's' }}
          </div>
          <div class="d-flex align-center font-mono node-id" @click="copyNodeId">
            {{ statusStore.id ? truncateId(statusStore.id, 8) : '—' }}
            <v-icon :icon="copied ? 'mdi-check' : 'mdi-content-copy'" size="14" class="ml-1" />
          </div>
        </div>
      </template>
    </v-navigation-drawer>

    <v-main>
      <v-container fluid class="pa-6">
        <router-view />
      </v-container>
    </v-main>
  </v-app>
</template>

<style scoped>
.font-mono {
  font-family: 'Roboto Mono', ui-monospace, monospace;
}
.node-id {
  font-size: 0.75rem;
  cursor: pointer;
  opacity: 0.85;
}
.node-id:hover {
  opacity: 1;
}
</style>
