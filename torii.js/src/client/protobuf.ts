/**
 * Dynamic protobuf encoding/decoding utilities
 * Handles conversion between JS objects and protobuf wire format
 */

// Wire types
const VARINT = 0;
const FIXED64 = 1;
const LENGTH_DELIMITED = 2;
const FIXED32 = 5;

// Schema types for type-safe encoding/decoding
export interface FieldSchema {
  number: number;
  type: string;
  repeated: boolean;
  optional?: boolean;
  messageType?: string;
  enumType?: string;
  mapKey?: string;
  mapValue?: string;
}

export interface MessageSchema {
  name: string;
  fullName: string;
  fields: Record<string, FieldSchema>;
}

// Schema registry for recursive message decoding
let schemaRegistry: Record<string, MessageSchema> = {};

export function setSchemaRegistry(registry: Record<string, MessageSchema>): void {
  schemaRegistry = registry;
}

export function getSchemaRegistry(): Record<string, MessageSchema> {
  return schemaRegistry;
}

/**
 * Encode a JS object to protobuf bytes
 *
 * Keys matching the "fN" pattern (e.g. f1, f4) are encoded with the
 * corresponding protobuf field number N.  This allows callers to skip
 * field numbers (e.g. {f1: …, f4: …}) without the gap being collapsed.
 *
 * Keys that do NOT match the "fN" pattern are assigned sequential field
 * numbers starting from 1 (backward-compatible with the old behaviour).
 */
export function encodeProtobufObject(obj: Record<string, unknown>): Uint8Array {
  const parts: number[] = [];
  let autoFieldNumber = 1;

  for (const key of Object.keys(obj)) {
    const match = key.match(/^f(\d+)$/);
    const fieldNumber = match ? parseInt(match[1], 10) : autoFieldNumber;
    autoFieldNumber = fieldNumber + 1;

    const value = obj[key];
    if (value === undefined || value === null) {
      continue;
    }

    const encoded = encodeField(fieldNumber, value);
    parts.push(...encoded);
  }

  return new Uint8Array(parts);
}

/**
 * Decode protobuf bytes to a JS object
 * Returns object with field numbers as keys (f1, f2, etc.)
 */
export function decodeProtobufObject(data: Uint8Array): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  let offset = 0;

  while (offset < data.length) {
    const [tag, newOffset] = readVarint(data, offset);
    offset = newOffset;

    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;

    const [value, valueOffset] = readField(data, offset, wireType);
    offset = valueOffset;

    const key = `f${fieldNumber}`;

    if (key in result) {
      const existing = result[key];
      if (Array.isArray(existing)) {
        existing.push(value);
      } else {
        result[key] = [existing, value];
      }
    } else {
      result[key] = value;
    }
  }

  return result;
}

function encodeField(fieldNumber: number, value: unknown): number[] {
  if (typeof value === 'string') {
    const bytes = new TextEncoder().encode(value);
    const tag = (fieldNumber << 3) | LENGTH_DELIMITED;
    return [...writeVarint(tag), ...writeVarint(bytes.length), ...bytes];
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      const tag = (fieldNumber << 3) | VARINT;
      // Handle negative numbers with zigzag encoding for sint32/sint64
      const encoded = value < 0 ? zigzagEncode(value) : value;
      return [...writeVarint(tag), ...writeVarint(encoded)];
    } else {
      // Float - encode as fixed32
      const tag = (fieldNumber << 3) | FIXED32;
      const buffer = new ArrayBuffer(4);
      new DataView(buffer).setFloat32(0, value, true);
      return [tag, ...new Uint8Array(buffer)];
    }
  }

  if (typeof value === 'bigint') {
    const tag = (fieldNumber << 3) | VARINT;
    return [...writeVarint(tag), ...writeVarintBigInt(value)];
  }

  if (typeof value === 'boolean') {
    const tag = (fieldNumber << 3) | VARINT;
    return [...writeVarint(tag), value ? 1 : 0];
  }

  if (value instanceof Uint8Array) {
    const tag = (fieldNumber << 3) | LENGTH_DELIMITED;
    return [...writeVarint(tag), ...writeVarint(value.length), ...value];
  }

  if (Array.isArray(value)) {
    const parts: number[] = [];
    for (const item of value) {
      parts.push(...encodeField(fieldNumber, item));
    }
    return parts;
  }

  if (typeof value === 'object') {
    const nested = encodeProtobufObject(value as Record<string, unknown>);
    const tag = (fieldNumber << 3) | LENGTH_DELIMITED;
    return [...writeVarint(tag), ...writeVarint(nested.length), ...nested];
  }

  return [];
}

function readField(data: Uint8Array, offset: number, wireType: number): [unknown, number] {
  switch (wireType) {
    case VARINT: {
      const [value, newOffset] = readVarint(data, offset);
      return [value, newOffset];
    }
    case FIXED64: {
      const view = new DataView(data.buffer, data.byteOffset + offset, 8);
      const value = view.getBigUint64(0, true);
      return [value, offset + 8];
    }
    case LENGTH_DELIMITED: {
      const [length, lengthOffset] = readVarint(data, offset);
      const bytes = data.slice(lengthOffset, lengthOffset + length);

      // Try to decode as nested message first, fall back to string/bytes
      const decoded = tryDecodeNested(bytes);
      return [decoded, lengthOffset + length];
    }
    case FIXED32: {
      const view = new DataView(data.buffer, data.byteOffset + offset, 4);
      const value = view.getUint32(0, true);
      return [value, offset + 4];
    }
    default:
      throw new Error(`Unknown wire type: ${wireType}`);
  }
}

function tryDecodeNested(bytes: Uint8Array): unknown {
  // Try to decode as nested protobuf message first.
  // Real messages in this codebase have multiple fields (Transfer: 7,
  // NftTransfer: 7, TokenTransfer: 10, Stats: 4, TopicInfo: 4, etc.).
  // Random binary data that happens to start with a valid tag rarely
  // produces 3+ sequential fields, so we use that as a discriminator.
  if (bytes.length >= 4) {
    const firstByte = bytes[0];
    const wireType = firstByte & 0x07;
    const fieldNum = firstByte >> 3;

    if (fieldNum >= 1 && fieldNum <= 15 && (wireType === 0 || wireType === 1 || wireType === 2 || wireType === 5)) {
      try {
        const nested = decodeProtobufObject(bytes);
        const keys = Object.keys(nested);
        if (keys.length >= 3 && keys.every(k => /^f\d+$/.test(k))) {
          const fieldNums = keys.map(k => parseInt(k.slice(1), 10)).sort((a, b) => a - b);
          if (fieldNums[0] <= 2) {
            const maxGap = fieldNums.length > 1
              ? Math.max(...fieldNums.slice(1).map((n, i) => n - fieldNums[i]))
              : 0;
            if (maxGap <= 5) {
              return nested;
            }
          }
        }
      } catch {
        // Not a valid nested message
      }
    }
  }

  // Try as UTF-8 string – only if every byte is printable
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    if (text.length === 0 || /^[\x20-\x7E\u00A0-\uFFFF\s]*$/.test(text)) {
      return text;
    }
  } catch {
    // Not valid UTF-8
  }

  return bytes;
}

function writeVarint(value: number): number[] {
  const bytes: number[] = [];
  while (value > 0x7f) {
    bytes.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  bytes.push(value & 0x7f);
  return bytes.length ? bytes : [0];
}

function writeVarintBigInt(value: bigint): number[] {
  const bytes: number[] = [];
  while (value > 0x7fn) {
    bytes.push(Number(value & 0x7fn) | 0x80);
    value >>= 7n;
  }
  bytes.push(Number(value & 0x7fn));
  return bytes.length ? bytes : [0];
}

export function readVarint(data: Uint8Array, offset: number): [number, number] {
  let result = 0;
  let shift = 0;
  let byte: number;

  do {
    if (offset >= data.length) {
      throw new Error('Varint extends beyond buffer');
    }
    byte = data[offset++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
  } while (byte & 0x80);

  return [result >>> 0, offset];
}

function zigzagEncode(value: number): number {
  return (value << 1) ^ (value >> 31);
}

/**
 * Frame a message for gRPC-Web transport
 */
export function frameMessage(message: Uint8Array): Uint8Array {
  const frame = new Uint8Array(5 + message.length);
  frame[0] = 0x00; // Data frame, not compressed
  const len = message.length;
  frame[1] = (len >> 24) & 0xff;
  frame[2] = (len >> 16) & 0xff;
  frame[3] = (len >> 8) & 0xff;
  frame[4] = len & 0xff;
  frame.set(message, 5);
  return frame;
}

/**
 * Encode with schema - maps field names to field numbers
 */
export function encodeWithSchema<T extends Record<string, unknown>>(
  obj: T,
  schema: MessageSchema
): Uint8Array {
  const parts: number[] = [];

  for (const [fieldName, fieldSchema] of Object.entries(schema.fields)) {
    const value = obj[fieldName];
    if (value === undefined || value === null) continue;

    const encoded = encodeFieldWithSchema(fieldSchema.number, value, fieldSchema);
    parts.push(...encoded);
  }

  return new Uint8Array(parts);
}

function encodeFieldWithSchema(
  fieldNumber: number,
  value: unknown,
  fieldSchema: FieldSchema
): number[] {
  // Handle nested messages with schema
  if (fieldSchema.type === 'message' && fieldSchema.messageType && typeof value === 'object' && value !== null) {
    if (fieldSchema.repeated && Array.isArray(value)) {
      const parts: number[] = [];
      for (const item of value) {
        const nestedSchema = schemaRegistry[fieldSchema.messageType];
        if (nestedSchema && typeof item === 'object' && item !== null) {
          const nested = encodeWithSchema(item as Record<string, unknown>, nestedSchema);
          const tag = (fieldNumber << 3) | LENGTH_DELIMITED;
          parts.push(...writeVarint(tag), ...writeVarint(nested.length), ...nested);
        } else {
          parts.push(...encodeField(fieldNumber, item));
        }
      }
      return parts;
    } else {
      const nestedSchema = schemaRegistry[fieldSchema.messageType];
      if (nestedSchema) {
        const nested = encodeWithSchema(value as Record<string, unknown>, nestedSchema);
        const tag = (fieldNumber << 3) | LENGTH_DELIMITED;
        return [...writeVarint(tag), ...writeVarint(nested.length), ...nested];
      }
    }
  }

  // Fall back to standard encoding
  return encodeField(fieldNumber, value);
}

/**
 * Decode with schema - maps field numbers to field names
 */
export function decodeWithSchema<T>(
  data: Uint8Array,
  schema: MessageSchema
): T {
  const raw = decodeProtobufObject(data);
  return mapFieldNumbersToNames<T>(raw, schema);
}

function mapFieldNumbersToNames<T>(
  raw: Record<string, unknown>,
  schema: MessageSchema
): T {
  const result: Record<string, unknown> = {};

  // Build number → name + schema lookup
  const numberToName: Record<number, string> = {};
  const numberToSchema: Record<number, FieldSchema> = {};
  for (const [name, field] of Object.entries(schema.fields)) {
    numberToName[field.number] = name;
    numberToSchema[field.number] = field;
  }

  for (const [key, value] of Object.entries(raw)) {
    const match = key.match(/^f(\d+)$/);
    if (!match) {
      result[key] = value;
      continue;
    }

    const fieldNum = parseInt(match[1], 10);
    const fieldName = numberToName[fieldNum];
    const fieldSchema = numberToSchema[fieldNum];

    if (!fieldName) {
      result[key] = value; // Unknown field, keep as-is
      continue;
    }

    // Handle google.protobuf.Any specially
    if (fieldSchema.messageType === 'Any') {
      let anyRaw: unknown = value;
      if (anyRaw instanceof Uint8Array) {
        try {
          anyRaw = decodeProtobufObject(anyRaw);
        } catch {
          result[fieldName] = { typeUrl: '', value: anyRaw };
          continue;
        }
      }

      if (typeof anyRaw !== 'object' || anyRaw === null) {
        result[fieldName] = { typeUrl: '', value: anyRaw };
        continue;
      }

      const anyValue = anyRaw as Record<string, unknown>;
      const typeUrl = String(anyValue.f1 ?? '');
      const innerValue = anyValue.f2;

      // Extract type name from URL: "type.googleapis.com/torii.sinks.log.LogEntry" → "LogEntry"
      const typeName = typeUrl.split('/').pop()?.split('.').pop() ?? '';
      const fullTypeName = typeUrl.split('/').pop() ?? '';

      // Try to find schema by short name or full name
      const innerSchema = schemaRegistry[typeName] || schemaRegistry[fullTypeName];

      if (innerSchema && innerValue instanceof Uint8Array) {
        try {
          result[fieldName] = {
            typeUrl,
            value: decodeWithSchema(innerValue, innerSchema),
          };
          continue;
        } catch {
          // Fall through and keep raw value below.
        }
      }

      if (innerSchema && typeof innerValue === 'object' && innerValue !== null) {
        result[fieldName] = {
          typeUrl,
          value: mapFieldNumbersToNames(innerValue as Record<string, unknown>, innerSchema),
        };
      } else {
        result[fieldName] = { typeUrl, value: innerValue };
      }
      continue;
    }

    // Handle nested messages recursively
    if (fieldSchema.type === 'message' && fieldSchema.messageType) {
      const nestedSchema = schemaRegistry[fieldSchema.messageType];
      if (nestedSchema) {
        if (fieldSchema.repeated && Array.isArray(value)) {
          result[fieldName] = value.map(v =>
            typeof v === 'object' && v !== null
              ? mapFieldNumbersToNames(v as Record<string, unknown>, nestedSchema)
              : v
          );
        } else if (typeof value === 'object' && value !== null) {
          result[fieldName] = mapFieldNumbersToNames(
            value as Record<string, unknown>,
            nestedSchema
          );
        } else {
          result[fieldName] = value;
        }
      } else {
        result[fieldName] = value;
      }
    } else if (fieldSchema.repeated && !Array.isArray(value)) {
      // Single value for repeated field → wrap in array
      result[fieldName] = [value];
    } else {
      result[fieldName] = value;
    }
  }

  return result as T;
}
