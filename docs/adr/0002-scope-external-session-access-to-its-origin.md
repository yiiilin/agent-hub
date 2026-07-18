---
status: superseded by ADR-0022
---

# Scope external Session access to its origin

A Session is owned by one Hub User and has either a Hub-native origin or an external origin. Hub-native Sessions have no synthetic External Platform or External Identity; external Sessions remain scoped to the External Platform, External Tenant, and External Identity that created them. The Hub User can manage all owned Sessions through Agent Hub, while an external integration can access only Sessions from its own origin unless an explicit transfer or sharing grant is created; this sacrifices automatic cross-platform continuation to prevent conversation data, attachments, and tool authority from crossing platform trust boundaries.
