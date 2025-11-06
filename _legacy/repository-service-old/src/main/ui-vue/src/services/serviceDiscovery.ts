/**
 * Service Discovery for Repository Service
 * 
 * Uses the generic ServiceDiscoveryClient to discover services in the platform.
 */

import { createServiceDiscoveryClient } from './ServiceDiscoveryClient'

// Create configured service discovery client for repository service
const { client: serviceDiscoveryClient, helpers: serviceHelpers } = createServiceDiscoveryClient({
  platformRegistrationBaseUrl: '/connect',
  debug: true, // Enable debug logging for development
  cacheDuration: 30000 // Cache for 30 seconds
})

// Export the client and helpers for use in components
export { serviceDiscoveryClient as serviceDiscovery, serviceHelpers }

// Re-export types for convenience
export type { DiscoveredService } from './ServiceDiscoveryClient'
