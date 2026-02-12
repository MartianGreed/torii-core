import { ToriiClient, GrpcTransport, setSchemaRegistry, type MessageSchema } from "@toriijs/sdk";
import { schemas } from "./schemas";

export const SERVER_URL = "http://localhost:3000";

export interface TokensClient {
  subscribeTopics: (
    clientId: string,
    topics: Array<{ topic: string; filters?: Record<string, string> }>,
    onUpdate: (update: unknown) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ) => Promise<() => void>;
  call: <T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    requestSchema: MessageSchema,
    responseSchema: MessageSchema
  ) => Promise<T>;
  disconnect: () => void;
  getVersion: () => Promise<{ version: string; buildTime: string }>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function schemaFromTypeUrl(typeUrl: string): MessageSchema | undefined {
  const fullTypeName = typeUrl.split("/").pop() ?? "";
  const shortTypeName = fullTypeName.split(".").pop() ?? "";
  return schemas[fullTypeName] ?? schemas[shortTypeName];
}

function mapFieldsWithSchema(value: unknown, schema: MessageSchema | undefined): unknown {
  if (!schema || !isRecord(value)) {
    return value;
  }

  const numberToName: Record<number, string> = {};
  for (const [fieldName, fieldSchema] of Object.entries(schema.fields)) {
    numberToName[fieldSchema.number] = fieldName;
  }

  const result: Record<string, unknown> = {};
  for (const [key, fieldValue] of Object.entries(value)) {
    const match = key.match(/^f(\d+)$/);
    if (!match) {
      result[key] = fieldValue;
      continue;
    }

    const fieldNum = Number(match[1]);
    const fieldName = numberToName[fieldNum] ?? key;
    result[fieldName] = fieldValue;
  }

  return result;
}

function normalizeAnyData(data: unknown): unknown {
  if (!isRecord(data)) {
    return data;
  }

  // Already normalized Any payload
  if (typeof data.typeUrl === "string") {
    const schema = schemaFromTypeUrl(data.typeUrl);
    return {
      ...data,
      value: mapFieldsWithSchema(data.value, schema),
    };
  }

  // Raw Any payload { f1: type_url, f2: value }
  if (typeof data.f1 === "string" && "f2" in data) {
    const typeUrl = data.f1;
    const schema = schemaFromTypeUrl(typeUrl);
    return {
      typeUrl,
      value: mapFieldsWithSchema(data.f2, schema),
    };
  }

  return data;
}

function normalizeTopicUpdate(update: unknown): unknown {
  if (!isRecord(update)) {
    return update;
  }

  return {
    ...update,
    data: normalizeAnyData(update.data),
  };
}

export function createTokensClient(url: string = SERVER_URL): TokensClient {
  setSchemaRegistry(schemas);
  const baseClient = new ToriiClient(url, {});
  const transport = new GrpcTransport(url);

  return {
    subscribeTopics: async (clientId, topics, onUpdate, onError, onConnected) => {
      return baseClient.subscribeTopics(
        clientId,
        topics,
        (update) => onUpdate(normalizeTopicUpdate(update)),
        onError,
        onConnected
      );
    },
    disconnect: baseClient.disconnect.bind(baseClient),
    getVersion: baseClient.getVersion.bind(baseClient),
    call: <T = Record<string, unknown>>(
      path: string,
      request: Record<string, unknown>,
      requestSchema: MessageSchema,
      responseSchema: MessageSchema
    ): Promise<T> => {
      return transport.unaryCall<T>(path, request, { requestSchema, responseSchema });
    },
  };
}
