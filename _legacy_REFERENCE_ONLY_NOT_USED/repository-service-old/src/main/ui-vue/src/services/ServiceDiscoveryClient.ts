/**
 * Generic Service Discovery Client
 * 
 * A reusable TypeScript class for discovering services through the platform-registration service.
 * This class can be used by any frontend service to discover other services in the platform.
 */

import { createClient } from '@connectrpc/connect'
import { createBinaryTransport, PlatformRegistration } from '@pipeline/proto-stubs'
import type { ServiceDetails, ServiceListResponse } from '@pipeline/proto-stubs'

// Service interface types
export interface DiscoveredService {
  id: string
  name: string
  host: string
  port: number
  type: 'GRPC' | 'HTTP' | 'WEBSOCKET'
  status: 'HEALTHY' | 'UNHEALTHY' | 'UNKNOWN'
  registeredAt?: string
  lastHealthCheck?: string
  metadata?: Record<string, string>
}

export interface ServiceDiscoveryOptions {
  /**
   * Base URL for the platform-registration service
   * Default: '/platform-registration' (routes through Traefik)
   */
  platformRegistrationBaseUrl?: string
  
  /**
   * Enable debug logging
   * Default: false
   */
  debug?: boolean
  
  /**
   * Cache duration for discovered services in milliseconds
   * Default: 30000 (30 seconds)
   */
  cacheDuration?: number
}

/**
 * Generic Service Discovery Client
 */
export class ServiceDiscoveryClient {
  private platformRegistrationClient: ReturnType<typeof createClient<typeof PlatformRegistration>>
  private cache: DiscoveredService[] = []
  private lastCacheUpdate: number = 0
  private options: Required<ServiceDiscoveryOptions>

  constructor(options: ServiceDiscoveryOptions = {}) {
    this.options = {
      platformRegistrationBaseUrl: '/platform-registration',
      debug: false,
      cacheDuration: 30000,
      ...options
    }

    // Create transport that routes to platform-registration service through Vite proxy
    const transport = createBinaryTransport(this.options.platformRegistrationBaseUrl)

    // Create the platform registration client
    this.platformRegistrationClient = createClient(PlatformRegistration, transport)
  }

  /**
   * Discover all registered services in the platform
   */
  async discoverServices(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    // Check cache first
    if (!forceRefresh && this.isCacheValid()) {
      if (this.options.debug) {
        console.log('[ServiceDiscovery] Returning cached services:', this.cache)
      }
      return this.cache
    }

    try {
      if (this.options.debug) {
        console.log('[ServiceDiscovery] Discovering services via platform-registration...')
      }
      
      const response = await this.platformRegistrationClient.listServices({}) as ServiceListResponse
      
      if (this.options.debug) {
        console.log('[ServiceDiscovery] Received response:', response)
      }
      
      if (!response.services || response.services.length === 0) {
        if (this.options.debug) {
          console.log('[ServiceDiscovery] No services discovered')
        }
        this.cache = []
        this.lastCacheUpdate = Date.now()
        return []
      }
      
      const discoveredServices = response.services.map((service: ServiceDetails) => ({
        id: service.serviceId,
        name: service.serviceName,
        host: service.host || 'localhost',
        port: service.port,
        type: 'GRPC' as const, // Assuming GRPC for now
        status: (service.isHealthy ? 'HEALTHY' : 'UNHEALTHY') as 'HEALTHY' | 'UNHEALTHY',
        registeredAt: service.registeredAt ? new Date(
          Number(service.registeredAt.seconds) * 1000 + 
          Number(service.registeredAt.nanos) / 1000000
        ).toISOString() : undefined,
        lastHealthCheck: service.lastHealthCheck ? new Date(
          Number(service.lastHealthCheck.seconds) * 1000 + 
          Number(service.lastHealthCheck.nanos) / 1000000
        ).toISOString() : undefined,
        metadata: service.metadata || {}
      }))
      
      // Update cache
      this.cache = discoveredServices
      this.lastCacheUpdate = Date.now()
      
      if (this.options.debug) {
        console.log('[ServiceDiscovery] Mapped discovered services:', discoveredServices)
      }
      
      return discoveredServices
    } catch (error) {
      console.error('[ServiceDiscovery] Failed to discover services:', error)
      // Return cached data if available, otherwise empty array
      return this.cache.length > 0 ? this.cache : []
    }
  }

  /**
   * Find a specific service by name
   */
  async findService(serviceName: string, forceRefresh: boolean = false): Promise<DiscoveredService | null> {
    const services = await this.discoverServices(forceRefresh)
    return services.find(service => service.name === serviceName) || null
  }

  /**
   * Find services by type
   */
  async findServicesByType(type: 'GRPC' | 'HTTP' | 'WEBSOCKET', forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.discoverServices(forceRefresh)
    return services.filter(service => service.type === type)
  }

  /**
   * Find healthy services only
   */
  async findHealthyServices(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.discoverServices(forceRefresh)
    return services.filter(service => service.status === 'HEALTHY')
  }

  /**
   * Find services by name pattern (includes)
   */
  async findServicesByPattern(pattern: string, forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.discoverServices(forceRefresh)
    return services.filter(service => service.name.includes(pattern))
  }

  /**
   * Get service URL for frontend routing
   * Returns the Traefik-routed URL for accessing the service's frontend
   */
  getServiceUrl(serviceName: string): string {
    return `/${serviceName}/`
  }

  /**
   * Get gRPC endpoint for a service
   * Returns the Connect-RPC endpoint routed through Traefik
   */
  getServiceGrpcEndpoint(serviceName: string): string {
    return `/${serviceName}/connect`
  }

  /**
   * Clear the service cache
   */
  clearCache(): void {
    this.cache = []
    this.lastCacheUpdate = 0
  }

  /**
   * Check if cache is still valid
   */
  private isCacheValid(): boolean {
    return this.cache.length > 0 && 
           (Date.now() - this.lastCacheUpdate) < this.options.cacheDuration
  }

  /**
   * Get cache status
   */
  getCacheStatus(): { isValid: boolean; lastUpdate: Date | null; serviceCount: number } {
    return {
      isValid: this.isCacheValid(),
      lastUpdate: this.lastCacheUpdate > 0 ? new Date(this.lastCacheUpdate) : null,
      serviceCount: this.cache.length
    }
  }
}

/**
 * Service-specific discovery helpers
 */
export class ServiceDiscoveryHelpers {
  constructor(private client: ServiceDiscoveryClient) {}

  /**
   * Discover processing modules (chunker, embedder, etc.)
   */
  async discoverProcessingModules(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.client.discoverServices(forceRefresh)
    return services.filter(service => 
      service.name.includes('chunker') || 
      service.name.includes('embedder') || 
      service.name.includes('parser') ||
      service.name.includes('sink')
    )
  }

  /**
   * Discover the pipestream engine
   */
  async discoverPipeStreamEngine(forceRefresh: boolean = false): Promise<DiscoveredService | null> {
    return await this.client.findService('pipestream-engine', forceRefresh)
  }

  /**
   * Discover OpenSearch services
   */
  async discoverOpenSearchServices(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.client.discoverServices(forceRefresh)
    return services.filter(service => 
      service.name.includes('opensearch') || 
      service.name.includes('search')
    )
  }

  /**
   * Discover all infrastructure services
   */
  async discoverInfrastructureServices(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const services = await this.client.discoverServices(forceRefresh)
    return services.filter(service => 
      service.name.includes('consul') ||
      service.name.includes('kafka') ||
      service.name.includes('mysql') ||
      service.name.includes('opensearch') ||
      service.name.includes('minio')
    )
  }

  /**
   * Discover application services (non-infrastructure)
   */
  async discoverApplicationServices(forceRefresh: boolean = false): Promise<DiscoveredService[]> {
    const infrastructureServices = await this.discoverInfrastructureServices(forceRefresh)
    const infrastructureNames = new Set(infrastructureServices.map(s => s.name))
    
    const allServices = await this.client.discoverServices(forceRefresh)
    return allServices.filter(service => !infrastructureNames.has(service.name))
  }
}

/**
 * Factory function to create a configured service discovery client
 */
export function createServiceDiscoveryClient(options?: ServiceDiscoveryOptions): {
  client: ServiceDiscoveryClient
  helpers: ServiceDiscoveryHelpers
} {
  const client = new ServiceDiscoveryClient(options)
  const helpers = new ServiceDiscoveryHelpers(client)
  
  return { client, helpers }
}
