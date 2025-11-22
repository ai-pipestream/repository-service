import { createRouter, createWebHistory } from 'vue-router'
import SearchView from './views/SearchView.vue'
import DashboardView from './views/DashboardView.vue'

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: DashboardView,
  },
  {
    path: '/search',
    name: 'Search',
    component: SearchView,
  },
]

const router = createRouter({
  history: createWebHistory('/repository/'),
  routes,
})

export default router
