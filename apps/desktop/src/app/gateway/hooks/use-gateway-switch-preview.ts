import { useEffect } from 'react'

import { $gatewaySwitching, wipeSessionListsForGatewaySwitch } from '@/store/gateway-switch'
import { setSessionsLoading } from '@/store/session'

const HOLD_MS = 1400
const GAP_MS = 1100

function wantsPreview(): boolean {
  if (!import.meta.env.DEV || typeof window === 'undefined') {
    return false
  }

  try {
    return new URLSearchParams(window.location.search).get('gateway-switch') === '1'
  } catch {
    return false
  }
}

/**
 * Dev-only: `?gateway-switch=1` loops the soft-switch wipe so sidebar skeletons
 * retrigger without tearing down a real backend. Pair with a live shell to
 * review the in-place UX.
 */
export function useGatewaySwitchPreview(): void {
  useEffect(() => {
    if (!wantsPreview()) {
      return
    }

    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | null = null

    const sleep = (ms: number) =>
      new Promise<void>(resolve => {
        timer = setTimeout(resolve, ms)
      })

    void (async () => {
      while (!cancelled) {
        $gatewaySwitching.set(true)
        wipeSessionListsForGatewaySwitch()
        await sleep(HOLD_MS)

        if (cancelled) {
          break
        }

        setSessionsLoading(false)
        $gatewaySwitching.set(false)
        await sleep(GAP_MS)
      }
    })()

    return () => {
      cancelled = true

      if (timer !== null) {
        clearTimeout(timer)
      }

      $gatewaySwitching.set(false)
    }
  }, [])
}
