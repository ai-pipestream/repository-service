<template>
  <NavShell
    :title="'Repository Service'"
    :auto-load-menu="true"
    :items-url="'/connect/system-nav/menu-items.json'"
  >
    <router-view />
  </NavShell>
</template>

<script setup lang="ts">
import { NavShell } from '@pipeline/shared-nav'
</script>

<style scoped>
.v-navigation-drawer {
  margin-top: 64px !important;
}
</style>