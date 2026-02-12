/**
 * ToriiClient - Generic plugin-based client for Torii gRPC services
 *
 * Usage:
 * ```typescript
 * import { ToriiClient } from "@toriijs/sdk";
 * import { SqlSinkClient, LogSinkClient } from "./generated";
 *
 * const client = new ToriiClient("http://localhost:8080", {
 *   sql: SqlSinkClient,
 *   log: LogSinkClient,
 * });
 *
 * // Fully typed access
 * const result = await client.sql.query("SELECT * FROM users");
 * ```
 */

import { BaseSinkClient } from './BaseSinkClient';
import { GrpcTransport } from './GrpcTransport';
import { decodeProtobufObject, decodeWithSchema, getSchemaRegistry } from './protobuf';

// Type for a client class constructor
type ClientClass<T extends BaseSinkClient = BaseSinkClient> = new (baseUrl: string) => T;

// Map of plugin names to client classes
type ClientMap = Record<string, ClientClass>;

// Transform client classes to instances
type InstantiatedClients<T extends ClientMap> = {
  [K in keyof T]: InstanceType<T[K]>;
};

export interface TopicSubscription {
  topic: string;
  filters?: Record<string, string>;
}

export interface TopicUpdate {
  topic: string;
  updateType: number;
  timestamp: number;
  typeId: string;
  data?: unknown;
}

export interface ToriiClientOptions<T extends ClientMap = ClientMap> {
  plugins?: T;
}

/**
 * Main Torii client with plugin support
 */
class ToriiClientImpl<T extends ClientMap = {}> {
  private _baseUrl: string;
  private _transport: GrpcTransport;
  private _clients: InstantiatedClients<T>;
  private _abortController: AbortController | null = null;

  constructor(baseUrl: string, plugins?: T) {
    this._baseUrl = baseUrl;
    this._transport = new GrpcTransport(baseUrl);

    // Instantiate all plugin clients
    this._clients = {} as InstantiatedClients<T>;
    if (plugins) {
      for (const [key, ClientClass] of Object.entries(plugins)) {
        (this._clients as Record<string, BaseSinkClient>)[key] = new ClientClass(baseUrl);
      }
    }
  }

  get baseUrl(): string {
    return this._baseUrl;
  }

  // ==================
  // Core Torii Methods
  // ==================

  /**
   * Get server version
   */
  async getVersion(): Promise<{ version: string; buildTime: string }> {
    const response = await this._transport.unaryCall<Record<string, unknown>>(
      '/torii.Torii/GetVersion',
      {}
    );
    // Proto fields: f1 = version, f2 = build_time
    return {
      version: String(response.f1 ?? ''),
      buildTime: String(response.f2 ?? ''),
    };
  }

  /**
   * List available topics from all registered sinks
   */
  async listTopics(): Promise<
    Array<{
      name: string;
      sinkName: string;
      availableFilters: string[];
      description: string;
    }>
  > {
    const response = await this._transport.unaryCall<Record<string, unknown>>(
      '/torii.Torii/ListTopics',
      {}
    );

    // Proto fields: f1 = topics (repeated)
    // TopicInfo: f1 = name, f2 = sink_name, f3 = available_filters, f4 = description
    const topics = response.f1;
    if (!topics) return [];

    const topicList = Array.isArray(topics) ? topics : [topics];
    return topicList.map((t: Record<string, unknown>) => ({
      name: String(t.f1 ?? ''),
      sinkName: String(t.f2 ?? ''),
      availableFilters: Array.isArray(t.f3) ? t.f3.map(String) : [],
      description: String(t.f4 ?? ''),
    }));
  }

  /**
   * Subscribe to topics with server-side streaming
   */
  async subscribeTopics(
    clientId: string,
    topics: TopicSubscription[],
    onUpdate: (update: TopicUpdate) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ): Promise<() => void> {
    this._abortController = new AbortController();
    const registry = getSchemaRegistry();
    const topicUpdateSchema = registry['torii.TopicUpdate'] || registry['TopicUpdate'];

    const topicsEncoded = topics.map((t) => {
      const sub: Record<string, unknown> = { f1: t.topic };
      if (t.filters && Object.keys(t.filters).length > 0) {
        sub.f2 = t.filters;
      }
      return sub;
    });

    const request: Record<string, unknown> = {
      f1: clientId,
      f2: topicsEncoded,
    };

    (async () => {
      try {
        for await (const response of this._transport.streamCall<Record<string, unknown>>(
          '/torii.Torii/SubscribeToTopicsStream',
          request,
          {
            abort: this._abortController?.signal,
            onConnected,
            responseSchema: topicUpdateSchema,
          }
        )) {
          const normalized = topicUpdateSchema
            ? response
            : {
                topic: String(response.f1 ?? ''),
                updateType: Number(response.f2 ?? 0),
                timestamp: Number(response.f3 ?? 0),
                typeId: String(response.f4 ?? ''),
                data: response.f5,
              };

          let data = (normalized as Record<string, unknown>).data;

          if (data && typeof data === 'object') {
            const anyData = data as Record<string, unknown>;
            const typeUrl =
              typeof anyData.typeUrl === 'string' ? (anyData.typeUrl as string) : '';
            const anyValue = anyData.value;

            if (typeUrl && anyValue instanceof Uint8Array) {
              const fullTypeName = typeUrl.split('/').pop() ?? '';
              const shortTypeName = fullTypeName.split('.').pop() ?? '';
              const payloadSchema = registry[fullTypeName] || registry[shortTypeName];

              if (payloadSchema) {
                try {
                  data = {
                    typeUrl,
                    value: decodeWithSchema(anyValue, payloadSchema),
                  };
                } catch {
                  data = {
                    typeUrl,
                    value: decodeProtobufObject(anyValue),
                  };
                }
              } else {
                try {
                  data = {
                    typeUrl,
                    value: decodeProtobufObject(anyValue),
                  };
                } catch {
                  // Keep raw Any when payload cannot be decoded.
                }
              }
            }
          } else if (data instanceof Uint8Array) {
            // Backward compatibility if no schema is registered.
            try {
              data = decodeProtobufObject(data);
            } catch {
              // Leave as raw bytes
            }
          }

          onUpdate({
            topic: String((normalized as Record<string, unknown>).topic ?? ''),
            updateType: Number((normalized as Record<string, unknown>).updateType ?? 0),
            timestamp: Number((normalized as Record<string, unknown>).timestamp ?? 0),
            typeId: String((normalized as Record<string, unknown>).typeId ?? ''),
            data,
          });
        }
      } catch (err: unknown) {
        if (err instanceof Error && err.name !== 'AbortError') {
          onError?.(err);
        }
      }
    })();

    return () => {
      this._abortController?.abort();
      this._abortController = null;
    };
  }

  /**
   * Disconnect any active subscriptions
   */
  disconnect(): void {
    this._abortController?.abort();
    this._abortController = null;
  }
}

// Create the ToriiClient with Proxy for plugin access
export function createToriiClient<T extends ClientMap>(
  baseUrl: string,
  plugins?: T
): ToriiClientImpl<T> & InstantiatedClients<T> {
  const client = new ToriiClientImpl(baseUrl, plugins);

  return new Proxy(client, {
    get(target, prop, receiver) {
      // First check if it's a property of the client itself
      if (prop in target) {
        const value = Reflect.get(target, prop, receiver);
        if (typeof value === 'function') {
          return value.bind(target);
        }
        return value;
      }
      // Then check if it's a plugin
      if (prop in (target as any)._clients) {
        return (target as any)._clients[prop];
      }
      return undefined;
    },
  }) as ToriiClientImpl<T> & InstantiatedClients<T>;
}

// Export a class-like interface for familiar usage pattern
export const ToriiClient = createToriiClient as unknown as {
  new <T extends ClientMap = {}>(
    baseUrl: string,
    plugins?: T
  ): ToriiClientImpl<T> & InstantiatedClients<T>;
};

// Type declaration to make ToriiClient work as a class
export type ToriiClient<T extends ClientMap = {}> = ToriiClientImpl<T> &
  InstantiatedClients<T>;
