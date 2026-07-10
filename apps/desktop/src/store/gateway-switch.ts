import { atom } from 'nanostores'

import { queryClient } from '@/lib/query-client'
import { resetSessionsLimit } from '@/store/layout'
import { requestFreshSession } from '@/store/profile'
import {
  setActiveSessionId,
  setAttentionSessionIds,
  setCronSessions,
  setMessages,
  setMessagingPlatformTotals,
  setMessagingSessions,
  setMessagingTruncated,
  setSelectedStoredSessionId,
  setSessionProfileTotals,
  setSessions,
  setSessionsLoading,
  setSessionsTotal,
  setWorkingSessionIds
} from '@/store/session'

// True while a soft gateway-mode apply is mid-flight (wipe → re-dial). Lets the
// boot hook suppress the backend-exit toast and keeps the cold-boot CONNECTING
// overlay from resurrecting when startHermes re-emits boot progress.
export const $gatewaySwitching = atom(false)

/**
 * Clear gateway-bound session UI so sidebar skeletons retrigger.
 *
 * Sessions live in nanostores (not React Query) — refreshSessions merges into
 * the existing list, so without an explicit wipe a soft switch would keep
 * painting the previous gateway's rows. RQ caches (settings/config/skills) are
 * invalidated separately; the live session list is this path.
 */
export function wipeSessionListsForGatewaySwitch(): void {
  setSessions([])
  setSessionsTotal(0)
  setSessionProfileTotals({})
  setCronSessions([])
  setMessagingSessions([])
  setMessagingPlatformTotals({})
  setMessagingTruncated(false)
  setWorkingSessionIds([])
  setAttentionSessionIds([])
  setSessionsLoading(true)
  resetSessionsLimit()

  setActiveSessionId(null)
  setSelectedStoredSessionId(null)
  setMessages([])
  requestFreshSession()

  void queryClient.invalidateQueries()
}
