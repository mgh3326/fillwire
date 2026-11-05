// Package kst is Korea Standard Time, the one zone KRX and KIS timestamps use.
// Korea has no daylight saving, so a fixed offset is exact.
package kst

import "time"

// Offset is KST's fixed offset from UTC.
const Offset = 9 * time.Hour

// Zone is the KST location. Every package converts with this value so that
// formatted dates and zone names are identical across the runtime.
var Zone = time.FixedZone("KST", int(Offset/time.Second))
