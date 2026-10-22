/**
 * Shared assertions for SDK control-plane failures: every one carries the
 * implementation-defined JSON-RPC code plus a stable `data.reason` token, so a
 * client never has to match error text.
 */

import { expect } from 'vitest'
import { JsonRpcResponseError, SDK_ERROR_CODE } from '@deepseek-ai/dsh-sdk-protocol'

/** Read the structured reason from one thrown value, or undefined when absent. */
function reasonOf(error: unknown): string | undefined {
  if (!(error instanceof JsonRpcResponseError)) return undefined
  const data = error.data
  if (typeof data !== 'object' || data === null || !('reason' in data)) return undefined
  const reason = data.reason
  return typeof reason === 'string' ? reason : undefined
}

/**
 * Assert one synchronous call fails with the expected control-plane classification.
 * @param call - the call expected to throw.
 * @param reason - expected `error.data.reason`.
 */
export function expectSdkError(call: () => unknown, reason: string): void {
  try {
    call()
  } catch (error: unknown) {
    expect(error).toBeInstanceOf(JsonRpcResponseError)
    expect((error as JsonRpcResponseError).code).toBe(SDK_ERROR_CODE)
    expect(reasonOf(error)).toBe(reason)
    return
  }
  throw new Error(`expected a "${reason}" SDK error, but the call succeeded`)
}

/**
 * Assert one asynchronous call fails with the expected control-plane classification.
 * @param call - the promise expected to reject.
 * @param reason - expected `error.data.reason`.
 */
export async function expectSdkErrorAsync(call: Promise<unknown>, reason: string): Promise<void> {
  try {
    await call
  } catch (error: unknown) {
    expect(error).toBeInstanceOf(JsonRpcResponseError)
    expect((error as JsonRpcResponseError).code).toBe(SDK_ERROR_CODE)
    expect(reasonOf(error)).toBe(reason)
    return
  }
  throw new Error(`expected a "${reason}" SDK error, but the call resolved`)
}
