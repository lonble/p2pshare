<script setup lang="ts">
import { computed } from 'vue'
import type { Task } from '@/stores/files.ts'

const props = defineProps<{ tasks: Task[] }>()
const emit = defineEmits<{ dismiss: [id: string] }>()

const percent = (t: Task) => (t.total > 0 ? Math.round((t.done / t.total) * 100) : 0)

const icon = (t: Task) => (t.kind === 'publish' ? 'mdi-upload' : 'mdi-download')

const color = computed(() => (t: Task) => {
  if (t.status === 'error') return 'error'
  if (t.status === 'done') return 'success'
  return 'primary'
})
</script>

<template>
  <v-list v-if="props.tasks.length" density="compact" class="task-list" lines="two">
    <v-list-item v-for="t in props.tasks" :key="t.id">
      <template #prepend>
        <v-icon :icon="icon(t)" :color="color(t)" size="small" />
      </template>

      <v-list-item-title class="text-body-2">
        {{ t.kind === 'publish' ? 'Publishing' : 'Downloading' }}
        <span class="font-mono">{{ t.label }}</span>
      </v-list-item-title>

      <v-list-item-subtitle>
        <template v-if="t.status === 'error'">
          <span class="text-error">{{ t.error }}</span>
        </template>
        <template v-else>
          {{ t.done }}/{{ t.total || '?' }} chunks
        </template>
      </v-list-item-subtitle>

      <v-progress-linear
        :model-value="percent(t)"
        :color="color(t)"
        height="4"
        :indeterminate="t.status === 'running' && t.total === 0"
        rounded
      />

      <template #append>
        <v-btn
          v-if="t.status !== 'running'"
          icon="mdi-close"
          size="x-small"
          variant="text"
          @click="emit('dismiss', t.id)"
        />
      </template>
    </v-list-item>
  </v-list>
</template>

<style scoped>
.task-list {
  background: transparent;
}
.font-mono {
  font-family: 'Roboto Mono', ui-monospace, monospace;
  font-size: 0.85em;
}
</style>
