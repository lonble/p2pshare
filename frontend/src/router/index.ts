/**
 * router/index.ts
 *
 * Manual routes for ./src/pages/*.vue
 */

// Composables
import { createRouter, createWebHistory } from 'vue-router'
import FilesView from '@/views/FilesView.vue'
import PeersView from '@/views/PeersView.vue'
import BootstrapView from '@/views/BootstrapView.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { 
      path: '/', 
      name: 'files', 
      component: FilesView 
    },
    { 
      path: '/peers', 
      name: 'peers', 
      component: PeersView 
    },
    { 
      path: '/bootstrap', 
      name: 'bootstrap', 
      component: BootstrapView 
    },
  ],
})

export default router
