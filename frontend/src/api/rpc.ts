const STORAGE_KEY = 'p2pshare.rpcUrl'

function resolveRpcUrl(): string {
  const fallback = import.meta.env.VITE_RPC_URL ?? 'http://127.0.0.1:8001/'
  if (typeof window === 'undefined') return fallback

  const fromQuery = new URLSearchParams(window.location.search).get('rpc')
  if (fromQuery) {
    sessionStorage.setItem(STORAGE_KEY, fromQuery)
    return fromQuery
  }
  return sessionStorage.getItem(STORAGE_KEY) ?? fallback
}

export const RPC_URL = resolveRpcUrl()

/** Switches this tab to a different node and reloads to pick it up. */
export function setRpcUrl(url: string) {
  sessionStorage.setItem(STORAGE_KEY, url)
  window.location.reload()
}

export class RpcError extends Error {
  code: number

  constructor(code: number, message: string) {
    super(message)
    this.name = 'RpcError'
    this.code = code
  }
}

interface RpcEnvelope<T> {
  jsonrpc: string
  id: number
  result?: T
  error?: { code: number; message: string }
}

let nextId = 0

export async function rpcCall<T>(method: string, params?: unknown): Promise<T> {
  let res: Response
  try {
    res = await fetch(RPC_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: ++nextId, method, params }),
    })
  } catch {
    throw new Error(`Can't reach the node at ${RPC_URL}. Is it running?`)
  }

  if (!res.ok) {
    throw new Error(`Node returned HTTP ${res.status}`)
  }

  const body = (await res.json()) as RpcEnvelope<T>
  if (body.error) {
    throw new RpcError(body.error.code, body.error.message)
  }
  return body.result as T
}
