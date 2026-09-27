# Narrative Radar — Architecture technique

> Outil multi-chain (Solana → BNB Chain → Robinhood Chain) qui détecte les narratifs memecoin sur X,
> mesure la hype, confirme on-chain + smart money, décide (ENTER / SCALE / EXIT / IGNORE),
> mesure la performance depuis le first alert, et s'améliore chaque nuit sous contrôle humain.

Sommaire

0. [Principes directeurs](#0-principes-directeurs)
1. [Architecture technique (flux multi-chain)](#1-architecture-technique-flux-multi-chain)
2. [Priorisation MVP → V1 → V2](#2-priorisation-mvp--v1--v2)
3. [Agents et responsabilités](#3-agents-et-responsabilités)
4. [Format exact de l'état compact](#4-format-exact-de-létat-compact)
5. [Fusion Twitter + On-chain + Smart Money](#5-fusion-twitter--on-chain--smart-money)
6. [APIs prioritaires par chaîne](#6-apis-prioritaires-par-chaîne)
7. [Interface (dont « gains depuis first alert »)](#7-interface)
8. [Risques et mitigations](#8-risques-et-mitigations)
9. [Ordre de construction recommandé](#9-ordre-de-construction-recommandé)
- [Annexe A — Modèle de données](#annexe-a--modèle-de-données)
- [Annexe B — Points à vérifier avant de coder](#annexe-b--points-à-vérifier-avant-de-coder)

---

## 0. Principes directeurs

| # | Principe | Conséquence concrète |
|---|----------|----------------------|
| P1 | **Les LLM perçoivent, le code décide des seuils, l'humain signe.** | Les agents LLM produisent des *labels*, jamais des ordres. L'exécution passe par un service séparé sans LLM. |
| P2 | **Aucun modèle ne touche une clé privée.** | Signer isolé (processus/infra distincte), accessible uniquement via une API de politique (limites, allowlist). |
| P3 | **Tout est discrétisé en adjectifs avant la décision.** | Un seul contrat d'entrée pour le moteur de décision : l'état compact (§4), versionné et validé par schéma. |
| P4 | **Chaque alerte est un pari enregistré.** | Timestamp + prix de référence figés au first alert, suivis ensuite sans exception (y compris les IGNORE) pour éviter le biais du survivant. |
| P5 | **Chain-agnostic au cœur, chain-specific aux bords.** | Un `ChainAdapter` par chaîne ; tout le reste (fusion, décision, tracking, UI) ne connaît que des événements normalisés. |
| P6 | **Déterministe d'abord, IA ensuite.** | Le MVP doit fonctionner avec des règles simples ; les LLM améliorent la perception, pas la tuyauterie. |

---

## 1. Architecture technique (flux multi-chain)

### 1.1 Vue d'ensemble

```
                         ┌──────────────────────────── INGESTION ────────────────────────────┐
                         │                                                                    │
  X / Twitter ──► [X Ingestor] ──► tweets.raw ─┐                                              │
  (Grok live search,                           │                                              │
   X API stream)                               │                                              │
                                               ▼                                              │
  Solana  ──► [SolanaAdapter]  ─┐       ┌──────────────┐                                      │
  BNB     ──► [BnbAdapter]     ─┼─────► │  Event Bus   │  (Redis Streams MVP → NATS/Kafka V2) │
  Robinhood ► [RobinhoodAdapter]┘       │  topics:     │                                      │
                                        │  tweets.*    │                                      │
  AlphaWallets / GMGN / Cielo ──►       │  chain.*     │                                      │
  [SmartMoney Ingestor] ──────────────► │  wallets.*   │                                      │
                                        └──────┬───────┘                                      │
                         └─────────────────────┼──────────────────────────────────────────────┘
                                               │
                  ┌──────────── PERCEPTION (agents) ─────────────┐
                  │                                              │
                  │  Batch Filter (Quail / AI-SQL) ── tri du bruit texte
                  │  Narrative Tracker ─ Hype Velocity ─ KOL Watcher
                  │  Early Signal ─ Smart Money Cross-Checker ─ Risk Cluster
                  │  On-chain Feature Builder (déterministe)     │
                  │  Timing Agent (heures chaudes)               │
                  └──────────────────────┬───────────────────────┘
                                         │ features numériques + labels
                                         ▼
                  ┌────────────── FUSION & ÉTAT ─────────────────┐
                  │  Entity Resolver (tweet ↔ token ↔ narratif)   │
                  │  Signal State Machine (par token)             │
                  │  Adjectivizer (numérique → adjectifs, §4)     │
                  └──────────────────────┬───────────────────────┘
                                         │ état compact (≤15 mots)
                                         ▼
                  ┌──────────────── DÉCISION ────────────────────┐
                  │  Jev / Kev (moteur typé)  +  Rulebook versionné│
                  │  → ENTER | SCALE | EXIT | IGNORE + confiance   │
                  └───────┬───────────────────────────┬──────────┘
                          │                           │
                          ▼                           ▼
             ┌──── PERFORMANCE ────┐        ┌──── UI / NOTIFS ─────┐
             │ First-alert ledger  │◄──────►│ Web app (signal card, │
             │ Price snapshots     │        │ Outcomes), Telegram   │
             │ Gains since alert   │        └──────────┬────────────┘
             └─────────┬───────────┘                   │ clic humain
                       │                               ▼
                       │                  ┌──── EXÉCUTION (isolée) ────┐
                       │                  │ Policy Engine (limites)     │
                       │                  │ Signer (clés) — aucun LLM   │
                       │                  │ Multi-wallet router          │
                       │                  └─────────────────────────────┘
                       ▼
             ┌──── NIGHTLY LOOP ─────────────────────────────┐
             │ Claude (Opus) : analyse erreurs haute confiance │
             │ → propositions de règles → backtest → GATE HUMAIN│
             │ → Rulebook vN+1                                  │
             └──────────────────────────────────────────────────┘
```

### 1.2 Stack recommandée

| Couche | MVP | V1/V2 | Pourquoi |
|--------|-----|-------|----------|
| Langage | **TypeScript** (Node 22) partout + **Zod** pour tous les contrats | Rust pour l'ingestion Solana gRPC si latence critique | Type-safety bout-en-bout, cohérent avec un moteur « TypeSafe » (Jev), un seul langage à maintenir |
| Monorepo | pnpm workspaces + turborepo | idem | `packages/core`, `packages/adapters/*`, `apps/*` |
| Bus d'événements | **Redis Streams** | NATS JetStream ou Redpanda | Redis suffit < 5k evt/s ; consumer groups, replay |
| Base | **Postgres 16 + TimescaleDB** | + ClickHouse pour l'analytique lourde | Hypertables pour prix/snapshots, SQL classique pour le reste |
| Cache / état chaud | Redis | idem | Fenêtres glissantes (vélocité), déduplication |
| Jobs planifiés | BullMQ (Redis) | Temporal | Snapshots de prix, nightly loop |
| UI | **Next.js** + Server-Sent Events | idem | Temps réel simple, pas de WebSocket à gérer au début |
| Notifs | Bot Telegram | + push mobile | Low-friction, boutons inline = 1 clic |
| Exécution | *(MVP : aucune — deep links)* | Service Go/TS isolé + signer (Turnkey / Privy server wallets / KMS) | Isolation réseau et IAM stricte |
| Observabilité | pino logs + Grafana/Prometheus | + OpenTelemetry | Latence tweet→alerte = KPI n°1 |
| Déploiement | 1 VPS (Docker Compose) proche des RPC | K8s léger / Fly.io | Le MVP doit tourner sur une machine |

### 1.3 Structure du repo cible

```
narrative-radar/
├── packages/
│   ├── core/            # types Zod : NormalizedEvent, TokenFeatures, CompactState, Decision
│   ├── adjectivizer/    # numérique → adjectifs (tables de seuils versionnées)
│   ├── decision/        # interface DecisionEngine + adaptateur Jev/Kev + rulebook
│   ├── adapters/
│   │   ├── solana/      # Helius/Yellowstone, pump.fun, PumpSwap/Raydium, Meteora
│   │   ├── bnb/         # four.meme, PancakeSwap, GoPlus
│   │   └── robinhood/   # adaptateur EVM générique (V2)
│   ├── smartmoney/      # AlphaWallets, Cielo, GMGN, clustering
│   └── agents/          # prompts + clients Grok/Claude, sorties typées
├── apps/
│   ├── ingest/          # workers d'ingestion
│   ├── brain/           # fusion, state machine, décision
│   ├── tracker/         # first-alert ledger + snapshots
│   ├── nightly/         # boucle d'amélioration
│   ├── executor/        # (V1) isolé, déployé à part, aucun import de packages/agents
│   └── web/             # Next.js
└── infra/               # docker-compose, migrations SQL
```

### 1.4 Le contrat `ChainAdapter` (cœur du multi-chain)

```ts
type Chain = "solana" | "bnb" | "robinhood";

interface ChainAdapter {
  chain: Chain;
  // flux
  onLaunch(cb: (e: LaunchEvent) => void): Unsubscribe;          // nouveau token / curve créée
  onCurveProgress(cb: (e: CurveEvent) => void): Unsubscribe;    // % bonding curve, achats/ventes
  onMigration(cb: (e: MigrationEvent) => void): Unsubscribe;    // graduation → DEX
  onSwap(tokenFilter: TokenId[], cb: (e: SwapEvent) => void): Unsubscribe;
  // requêtes
  getTokenSnapshot(t: TokenId): Promise<TokenSnapshot>;         // prix, liq, vol, mcap, âge
  getHolderDistribution(t: TokenId): Promise<HolderStats>;      // top10%, dev%, bundles, fresh wallets
  getSecurity(t: TokenId): Promise<SecurityFlags>;              // mint/freeze authority, honeypot, taxes
  getPriceAt(t: TokenId, ts: Date): Promise<PricePoint>;        // pour le tracking
}
```

Tout ce qui sort d'un adaptateur est normalisé (`TokenId = "${chain}:${address}"`, prix en USD + en natif, timestamps en ms UTC, `source` + `slot/block`).

### 1.5 Budget de latence cible (Solana)

| Étape | Cible MVP | Cible V1 |
|-------|-----------|----------|
| Tweet publié → ingéré | 30–90 s (polling Grok) | < 10 s (stream) |
| Launch on-chain → événement | < 3 s | < 1 s (gRPC) |
| Événement → état compact → décision | < 2 s | < 500 ms |
| Décision → notif UI/Telegram | < 1 s | < 300 ms |

---

## 2. Priorisation MVP → V1 → V2

### MVP (≈ 4–6 semaines, 1–2 devs) — « Voir, alerter, mesurer »

Objectif : prouver que les alertes ont un edge mesurable. **Pas d'exécution automatique.**

| Inclus | Détail |
|--------|--------|
| Chaîne | **Solana uniquement** (pump.fun + PumpSwap/Raydium) |
| Twitter | Grok (xAI API, recherche X live) en polling sur une watchlist KOL + requêtes narratives ; 3 agents : **Narrative Tracker, Hype Velocity, KOL Watcher** |
| On-chain | Launches, % bonding curve, liquidité, volume, âge, top10 holders, dev %, migration, mint/freeze authority |
| Smart money | **AlphaWallets.fun** (si API/export disponible) sinon liste de wallets importée manuellement + suivi via webhooks Helius |
| Décision | Moteur de règles **déterministe typé** derrière l'interface `DecisionEngine` (branchement Jev/Kev prévu) |
| État compact | Format complet §4 dès le jour 1 (c'est le contrat, il ne doit pas bouger) |
| Tracking | First-alert ledger + snapshots +5m/+15m/+1h/+4h/+24h, ATH/drawdown depuis alerte |
| UI | 1 page : signal courant + liste Outcomes ; Telegram avec boutons |
| Actions | **Deep links** (Jupiter / Axiom / Photon / GMGN) + bouton « Mark as taken » (saisie manuelle de la taille) |
| Nightly | Rapport nocturne *lecture seule* (Claude) : top erreurs, pas encore de propositions de règles |

Critère de sortie du MVP : ≥ 200 alertes trackées, distribution des gains depuis first alert visible, latence tweet→alerte mesurée.

### V1 (≈ +6–8 semaines) — « Décider mieux, agir vite »

- **BNB Chain** (four.meme + PancakeSwap, GoPlus pour honeypot/tax).
- Agents restants : **Early Signal, Smart Money Cross-Checker, Risk Cluster**.
- Smart money complet : AlphaWallets + **Cielo** + **GMGN** (en source secondaire) + **clustering de wallets** (financement commun, timing d'achat corrélé).
- **Batch Intelligence (Quail / AI-SQL)** : pré-filtrage du volume de tweets avant les agents coûteux.
- **Jev ou Kev** branché réellement en remplacement/complément du moteur de règles (mode shadow d'abord : les deux décident, on compare).
- **Nightly loop complet** : propositions de règles → backtest automatique → gate humaine dans l'UI.
- **Exécution 1-clic** : service executor isolé, signer externe, politique de risque, multi-wallet.
- Timing : heures chaudes calculées depuis les données propres, par chaîne.
- Stream X temps réel (API officielle ou fournisseur) si le budget le permet.

### V2 (≈ +8–12 semaines) — « Échelle et raffinement »

- **Robinhood Chain** via adaptateur EVM générique (dès que DEX/launchpads/indexeurs sont matures — voir Annexe B).
- Ingestion Solana en **Yellowstone gRPC** (latence < 1 s), NATS/Redpanda, ClickHouse.
- Narratifs cross-chain (même narratif, tokens sur plusieurs chaînes → qui mène ?).
- SCALE / EXIT automatiques sous politique (trailing stops, take-profit par paliers) — toujours bornés par le policy engine.
- Scoring des KOL par performance historique réelle (leurs calls vs gains depuis leur tweet).
- Mode multi-utilisateurs / partage de signaux (si produit).
- Fine-tuning des seuils de l'adjectivizer par chaîne et par régime de marché.

---

## 3. Agents et responsabilités

Règle commune : **chaque agent a une entrée typée, une sortie typée (Zod), un budget de coût, une fréquence, et n'a aucun accès réseau vers l'exécution.** Les sorties LLM sont contraintes à des enums (structured outputs) — pas de texte libre qui descend vers la décision.

### 3.1 Couche Twitter / Narrative (« Grokbot Agents »)

| Agent | Entrée | Sortie (typée) | Fréquence | Moteur |
|-------|--------|----------------|-----------|--------|
| **Narrative Tracker** | Tweets filtrés (fenêtre 30 min) | `Narrative { id, label, keywords[], tickers[], contracts[], firstSeenAt, stage: emerging\|forming\|established\|saturated }` — clustering de sujets (ex. « chien avec chapeau », « IA agent », actu politique) | toutes les 1–2 min | Embeddings + clustering (HDBSCAN) pour le groupement ; Grok pour nommer / résumer le cluster |
| **Hype Velocity Agent** | Compteurs de mentions par narratif/token (fenêtres 5/15/60 min), engagement pondéré | `phase: rising\|peaking\|fading\|dormant`, `acceleration` (dérivée 2nde) | toutes les 30 s | **Déterministe** : vélocité = mentions/5min ; accélération = Δvélocité ; peaking = vélocité haute + accélération ≤ 0 ; fading = vélocité en baisse > 2 fenêtres. Pondération par qualité de compte (anti-bots) |
| **KOL Watcher** | Timeline d'une watchlist KOL (tiers A/B/C) | `KolEvent { kol, tier, tokenRef?, stance: bullish\|neutral\|exit, isFirstMention, historicalHitRate }` | polling 30–60 s | Grok (extraction CA/ticker + stance) ; hit-rate calculé depuis notre propre tracking |
| **Early Signal Agent** | Tweets de petits comptes à fort signal, premières mentions d'un CA/ticker, comptes « alpha callers » | `EarlySignal { tokenRef, firstMentionAt, mentionersQuality, crossesToKols: bool }` — détecte ce qui *va* monter avant les KOL | 1 min | Règles (1ère occurrence d'un CA) + Grok pour qualifier |
| **Smart Money Cross-Checker** | Narratif/token candidat + flux wallets | `smartMoney: none\|scouting\|accumulating\|distributing`, `walletsCount`, `clusterIds[]` | à chaque événement | **Déterministe** (jointure token ↔ achats wallets suivis) ; LLM non nécessaire |
| **Risk Cluster Agent** | Holders, deployer, financement des wallets, patterns de tweets (bots, copier-coller), sécurité contrat | `risk: clean\|moderate\|elevated\|severe`, `flags[]` (bundle, dev-dump-history, sniper-cluster, bot-hype, honeypot, mint-authority) | à chaque nouveau candidat + toutes les 5 min | Déterministe (graphe de financement, % bundle) + Grok pour détecter les campagnes de shill coordonnées |

### 3.2 Couche On-chain (déterministe, pas de LLM)

| Composant | Responsabilité |
|-----------|----------------|
| **Launch Watcher** (par chaîne) | Nouveaux tokens sur launchpads (pump.fun, letsbonk/LaunchLab, Meteora DBC ; four.meme ; …) |
| **Curve Tracker** | % de progression de la bonding curve, vitesse de remplissage, ratio achats/ventes |
| **Market Feature Builder** | Liquidité, volume 5m/1h, mcap/FDV, âge, nb de trades, nb d'acheteurs uniques |
| **Holder Analyzer** | Top10 %, dev %, bundles (achats même slot), fresh wallets %, snipers |
| **Migration Watcher** | Graduation → DEX (PumpSwap/Raydium, PancakeSwap), listing CEX éventuel |
| **Security Checker** | Mint/freeze authority (Solana), honeypot/tax/owner privileges (EVM via GoPlus) |

### 3.3 Couche Smart Money

| Composant | Responsabilité |
|-----------|----------------|
| **Wallet Registry** | Liste consolidée des wallets suivis, avec source (AlphaWallets prioritaire), tags (sniper, swing, insider), score de performance calculé en interne |
| **Wallet Stream** | Webhooks/stream des swaps de ces wallets (Helius webhooks Solana, logs EVM BNB) |
| **Cluster Detector** | Regroupe les wallets qui : sont financés par la même source, achètent dans les mêmes N blocs, ou partagent des patterns. Distingue *cluster pro indépendant* (bon signal) de *cluster insider/dev* (risque) |
| **Wallet Scorer** (nightly) | Recalcule PnL réalisé, win rate, temps de détention moyen ; déclasse les wallets devenus mauvais |

### 3.4 Couche Batch Intelligence

| Composant | Responsabilité |
|-----------|----------------|
| **Batch Filter (Quail / AI-SQL)** | Requêtes en langage naturel → SQL sur le stock de tweets (« tweets des 2 dernières heures mentionnant un CA Solana par des comptes < 5k followers avec > 20 likes »). Réduit le volume avant les agents LLM coûteux. Sert aussi à l'analyse ad-hoc et au nightly loop. |

Fallback si Quail ne convient pas : DuckDB/Postgres + classifieur LLM léger (Claude Haiku ou équivalent) en batch.

### 3.5 Couche Fusion / Décision

| Composant | Responsabilité |
|-----------|----------------|
| **Entity Resolver** | Lie tweets ↔ tokens ↔ narratifs (§5.1) |
| **Signal State Machine** | Un état par token : `detected → linked → confirmed → alerted → (entered) → exited/expired` |
| **Adjectivizer** | Transforme les features en état compact (§4), tables de seuils versionnées par chaîne |
| **Decision Engine (Jev/Kev)** | Consomme l'état compact + rulebook → `Decision { action, confidence, ruleIds[] }` |
| **Timing Agent** | Fournit `timing: hot\|warm\|cold` selon heure/jour et chaîne |

### 3.6 Couche Performance & Amélioration

| Composant | Responsabilité |
|-----------|----------------|
| **First-Alert Ledger** | Enregistre de façon immuable (append-only) le first alert (§7.3) |
| **Snapshot Scheduler** | Prix à +1m, +5m, +15m, +1h, +4h, +24h, +7d ; suivi continu ATH/ATL pendant 24h |
| **Nightly Reviewer** (Claude Opus) | Analyse des erreurs à haute confiance, propositions de règles, rapport |
| **Backtester** | Rejoue les états compacts historiques avec le rulebook proposé |
| **Human Gate** | UI d'approbation : diff de règles + impact backtest → Approve / Reject |

---

## 4. Format exact de l'état compact

### 4.1 Règles

- **13 slots fixes, ordre fixe**, un mot par slot (+ préfixe de slot). 13 mots de valeur ≤ 15 → respecte la contrainte.
- Toutes les valeurs sont des **adjectifs** issus d'un vocabulaire fermé (enum). Le slot `chain` est l'unique exception (nom de chaîne, exigé par le cahier des charges).
- Sérialisation texte (pour Jev/LLM) **et** objet typé (pour le code) — les deux sont générés du même schéma.
- Versionné : `v1`. Toute modification = nouvelle version + recalibrage.

### 4.2 Schéma

| # | Slot | Valeurs autorisées (ordre croissant) | Source |
|---|------|--------------------------------------|--------|
| 1 | `chain` | `solana` · `bnb` · `robinhood` | adaptateur |
| 2 | `narrative` | `absent` · `emerging` · `forming` · `established` · `saturated` | Narrative Tracker |
| 3 | `hype` | `dormant` · `rising` · `peaking` · `fading` | Hype Velocity |
| 4 | `kol` | `silent` · `curious` · `active` · `unanimous` | KOL Watcher (pondéré par tier + hit-rate) |
| 5 | `early` | `absent` · `faint` · `strong` | Early Signal |
| 6 | `smart` | `absent` · `scouting` · `accumulating` · `distributing` | Smart Money Cross-Checker |
| 7 | `cluster` | `none` → `independent` · `coordinated` · `insider` | Cluster Detector *(valeur `none` = adjectif « aucun »)* |
| 8 | `curve` | `early` · `advanced` · `graduating` · `migrated` | Curve Tracker / Migration |
| 9 | `liquidity` | `thin` · `adequate` · `deep` | Market Features |
| 10 | `volume` | `quiet` · `healthy` · `surging` · `climactic` | Market Features (relatif à l'âge) |
| 11 | `holders` | `concentrated` · `balanced` · `distributed` | Holder Analyzer |
| 12 | `risk` | `clean` · `moderate` · `elevated` · `severe` | Risk Cluster + Security |
| 13 | `timing` | `cold` · `warm` · `hot` | Timing Agent |

> `age` n'est pas un slot séparé : il est *intégré* dans les seuils de `curve`, `volume` et `holders` (un token de 3 min avec 40 k$ de volume est « surging », un token de 3 jours avec le même volume est « quiet »). Si vous préférez l'exposer, remplacer `early` par `age: fresh|young|mature` — on reste à 13.

### 4.3 Sérialisation

**Texte (entrée Jev / LLM), 13 mots :**
```
chain:solana narrative:emerging hype:rising kol:curious early:strong smart:accumulating cluster:independent curve:advanced liquidity:adequate volume:surging holders:balanced risk:clean timing:hot
```

**Objet typé :**
```ts
import { z } from "zod";

export const CompactStateV1 = z.object({
  v: z.literal(1),
  chain: z.enum(["solana", "bnb", "robinhood"]),
  narrative: z.enum(["absent", "emerging", "forming", "established", "saturated"]),
  hype: z.enum(["dormant", "rising", "peaking", "fading"]),
  kol: z.enum(["silent", "curious", "active", "unanimous"]),
  early: z.enum(["absent", "faint", "strong"]),
  smart: z.enum(["absent", "scouting", "accumulating", "distributing"]),
  cluster: z.enum(["none", "independent", "coordinated", "insider"]),
  curve: z.enum(["early", "advanced", "graduating", "migrated"]),
  liquidity: z.enum(["thin", "adequate", "deep"]),
  volume: z.enum(["quiet", "healthy", "surging", "climactic"]),
  holders: z.enum(["concentrated", "balanced", "distributed"]),
  risk: z.enum(["clean", "moderate", "elevated", "severe"]),
  timing: z.enum(["cold", "warm", "hot"]),
});
export type CompactStateV1 = z.infer<typeof CompactStateV1>;

export const Decision = z.object({
  action: z.enum(["ENTER", "SCALE", "EXIT", "IGNORE"]),
  confidence: z.enum(["low", "medium", "high"]),
  ruleIds: z.array(z.string()),       // traçabilité : quelles règles ont tiré
  rulebookVersion: z.string(),
});
```

Le **contexte de position** (déjà en position ou non) n'est pas dans l'état compact : il est passé à part (`position: none|open|scaled`) car SCALE/EXIT n'ont de sens qu'avec une position ouverte. Le moteur reçoit donc `(CompactState, position)`.

### 4.4 Adjectivizer — exemple de table de seuils (Solana, v1, à calibrer)

```yaml
solana:
  liquidity:          # USD
    thin: [0, 15000]
    adequate: [15000, 80000]
    deep: [80000, inf]
  volume:             # volume 5 min / liquidité, modulé par l'âge
    quiet: [0, 0.2]
    healthy: [0.2, 1.0]
    surging: [1.0, 3.0]
    climactic: [3.0, inf]   # souvent signe de top
  holders:            # part top10 hors pool/curve
    concentrated: [0.35, 1]
    balanced: [0.20, 0.35]
    distributed: [0, 0.20]
  curve:              # % bonding curve pump.fun
    early: [0, 0.40]
    advanced: [0.40, 0.85]
    graduating: [0.85, 1.0]
    migrated: "migration_event"
```

Les tables sont des fichiers versionnés ; le nightly loop peut **proposer** de les modifier, jamais les modifier seul.

### 4.5 Exemple de règles (rulebook v1, déterministe)

```
R-001 IGNORE  si risk ∈ {severe} ou cluster = insider
R-002 IGNORE  si liquidity = thin et curve = migrated
R-010 ENTER   si hype = rising ∧ smart = accumulating ∧ risk ∈ {clean, moderate}
              ∧ narrative ∈ {emerging, forming}            → confidence high si timing = hot
R-011 ENTER   si early = strong ∧ kol ∈ {silent, curious} ∧ smart ∈ {scouting, accumulating}
              ∧ risk = clean                               → confidence medium
R-020 SCALE   si position = open ∧ hype = rising ∧ kol ∈ {active} ∧ volume = surging ∧ smart ≠ distributing
R-030 EXIT    si position ≠ none ∧ (hype = fading ∨ smart = distributing ∨ volume = climactic ∧ hype = peaking)
R-031 EXIT    si position ≠ none ∧ risk ∈ {elevated, severe}
R-099 IGNORE  par défaut
```
Priorité : les règles de risque (R-00x) et d'EXIT court-circuitent toujours ENTER/SCALE.

### 4.6 Jev / Kev

Le moteur est derrière une interface, ce qui rend le choix réversible :

```ts
interface DecisionEngine {
  name: string;
  decide(state: CompactStateV1, position: "none" | "open" | "scaled"): Promise<Decision>;
}
```

- **MVP** : `RulebookEngine` (implémentation TS pure des règles ci-dessus — rapide, auditable, zéro coût).
- **V1** : `JevEngine` ou `KevEngine` branché en **mode shadow** (il décide, on enregistre, on n'agit pas). Promotion quand son taux de décisions correctes dépasse le rulebook sur ≥ 2 semaines.
- Le fait que l'entrée soit un vocabulaire fermé de 13 mots rend l'espace d'états fini (~2 × 10⁷ combinaisons, dont une petite fraction réellement observée) : parfait pour un moteur typé, pour le backtest, et pour détecter les états jamais vus.

---

## 5. Fusion Twitter + On-chain + Smart Money

### 5.1 Entity resolution (tweet ↔ token ↔ narratif)

Ordre de confiance pour lier un tweet à un token :

1. **Adresse de contrat dans le tweet** (regex base58 32–44 car. pour Solana, `0x[a-f0-9]{40}` pour EVM) → lien certain ; la chaîne est déduite du format + vérifiée par `getTokenSnapshot`.
2. **Lien DexScreener / pump.fun / four.meme / GMGN** dans le tweet → parse de l'URL.
3. **`$TICKER`** → ambigu : on résout vers le token du même ticker **créé le plus récemment avec le plus de volume**, sur les chaînes actives, et on marque `confidence: low` tant que 1 ou 2 n'a pas confirmé.
4. **Narratif sans ticker** (ex. un mème viral) → recherche des tokens lancés dans les N dernières minutes dont nom/symbole/description matchent les mots-clés du narratif (embeddings). C'est la source du signal *le plus précoce* et le plus bruité.

### 5.2 Machine à états par token

```
          ┌────────────┐  tweet/narratif lié     ┌─────────┐
  (rien)──► DETECTED   ├────────────────────────►│ LINKED  │
          │ (on-chain  │                          │ tweet + │
          │  seul, ou  │◄─── launch correspondant │ token   │
          │  X seul)   │                          └────┬────┘
          └────────────┘                               │ ≥ 2 piliers verts
                                                       ▼
                                                 ┌───────────┐  décision ≠ IGNORE
                                                 │ CONFIRMED ├──────────────────► ALERTED (first alert figé)
                                                 └───────────┘                        │
                                                                                      ▼
                                                                  ENTERED → SCALED → EXITED / EXPIRED (72h)
```

### 5.3 Les trois piliers

| Pilier | « Vert » si… | Rôle |
|--------|--------------|------|
| **Narratif (X)** | `narrative ∈ {emerging, forming}` ∧ `hype = rising` (ou `early = strong`) | **Anticipation** — arrive en premier, le plus bruité |
| **On-chain** | `liquidity ≥ adequate` ∨ `curve ≥ advanced`, `volume ∈ {healthy, surging}`, `holders ≠ concentrated` | **Réalité** — l'argent suit-il ? |
| **Smart money** | `smart ∈ {scouting, accumulating}` ∧ `cluster ∈ {none, independent}` | **Validation** — les meilleurs sont-ils dedans ? |

**Veto** transversal : `risk ∈ {elevated, severe}` ou `cluster = insider` ⇒ IGNORE quels que soient les piliers.

Politique de confirmation :
- **3 piliers verts** → alerte *high*.
- **2 piliers verts** dont smart money → alerte *medium*.
- **Narratif + on-chain sans smart money** → alerte *low* (souvent le plus précoce ; utile à tracker pour apprendre).
- **Smart money seul** (pas de narratif) → signal « silent accumulation », affiché séparément, pas d'alerte principale.
- **1 pilier** → reste DETECTED, pas d'alerte, mais **l'horodatage `first_seen` est conservé** pour mesurer a posteriori combien de temps on avait d'avance.

### 5.4 Pourquoi cette combinaison marche

- X est le **leading indicator** mais massivement manipulé (bots, shills payés) → jamais suffisant seul.
- On-chain est **objectif** mais **lagging** et manipulable (wash trading, bundles) → le Risk Cluster l'assainit.
- Smart money est le **meilleur filtre de qualité** mais peut être « front-run » ou copié (wallets connus = wallets surveillés par tous) → on score les wallets en continu et on privilégie les clusters *indépendants*.

---

## 6. APIs prioritaires par chaîne

> ⚠️ Les offres/pricing de ces services changent souvent. Vérifier disponibilité, quotas et CGU avant de coder (Annexe B).

### 6.1 Transverse

| Besoin | Priorité 1 | Alternatives | Notes |
|--------|------------|--------------|-------|
| X / Twitter | **xAI Grok API** (recherche X live) pour les agents Grokbot | **X API v2** officielle (filtered stream, palier Pro coûteux) ; fournisseurs tiers type twitterapi.io | Grok = perception + accès X en une API. Pour le temps réel strict, un stream est nécessaire (V1). Tiers : risque CGU/stabilité |
| Prix / pairs multi-chain | **DexScreener API** (gratuit, rate-limité) | **Birdeye** (Solana + BSC, payant, plus riche) ; GeckoTerminal | DexScreener pour démarrer, Birdeye dès que les quotas gênent |
| Smart money | **AlphaWallets.fun** | **Cielo** (API wallet tracking), **GMGN** (pas d'API publique officielle stable → source secondaire uniquement) , Nansen, Arkham | Toujours recopier les wallets dans *notre* registre et suivre leurs tx *nous-mêmes* on-chain : on ne dépend du fournisseur que pour la découverte |
| LLM | **Grok** (agents temps réel X), **Claude** (Haiku pour batch/classement, Opus pour le nightly) | — | Sorties structurées obligatoires |
| Batch texte | **Quail (AI-SQL)** | DuckDB + LLM batch | |

### 6.2 Solana (priorité 1)

| Besoin | Priorité 1 | Alternatives |
|--------|------------|--------------|
| RPC + streaming | **Helius** (RPC, WebSockets, webhooks, Enhanced Transactions, DAS) | Triton / QuickNode ; **Yellowstone gRPC** (Geyser) en V2 pour la latence |
| Launches pump.fun | Abonnement aux logs du programme pump.fun via Helius | PumpPortal (WebSocket tiers, rapide à intégrer en MVP) ; Bitquery |
| Autres launchpads | letsbonk (Raydium LaunchLab), Meteora DBC — logs programme | Bitquery |
| Migration | Événements PumpSwap / Raydium (création de pool) | DexScreener (nouvelle paire) |
| Holders | Helius DAS / `getTokenLargestAccounts` | Birdeye holders, Solscan API |
| Sécurité | Lecture directe mint/freeze authority ; **RugCheck API** | Birdeye security |
| Prix historique (tracking) | Nos propres swaps ingérés (source de vérité) | Birdeye OHLCV, GeckoTerminal |
| Exécution (V1) | **Jupiter** Swap API ; tx pump.fun directes pour la curve | Jito bundles pour la priorité |
| Deep links (MVP) | Jupiter, Axiom, Photon, GMGN | |

### 6.3 BNB Chain (priorité 2)

| Besoin | Priorité 1 | Alternatives |
|--------|------------|--------------|
| RPC | **NodeReal** (MegaNode) ou QuickNode — WebSocket `eth_subscribe logs` | Ankr, Chainstack |
| Launches | Événements du contrat **four.meme** (bonding curve) | Bitquery (supporte four.meme) |
| Migration | Création de paire **PancakeSwap** (v2 `PairCreated`, v3 `PoolCreated`) | DexScreener |
| Holders / tx | **Etherscan API v2** (multichain, inclut BSC) | Moralis, Covalent/GoldRush |
| Sécurité | **GoPlus Security API** (honeypot, taxes, owner privileges) | Honeypot.is |
| Prix | Swaps ingérés + DexScreener/Birdeye | GeckoTerminal |
| Exécution (V1) | PancakeSwap router / agrégateur **1inch** ou **0x** | OpenOcean |

### 6.4 Robinhood Chain (priorité 3)

Robinhood Chain est un L2 basé sur la stack Arbitrum (Orbit). L'écosystème memecoin (launchpads, DEX, indexeurs) y est bien moins mature que Solana/BNB : **à réévaluer au lancement de V2**.

| Besoin | Approche |
|--------|----------|
| RPC | RPC officiel + fournisseur supportant la chaîne (vérifier Alchemy / QuickNode / Conduit) |
| Launches / DEX | **Adaptateur EVM générique** : écoute `PairCreated`/`PoolCreated` des DEX déployés sur la chaîne + contrats de launchpad identifiés à ce moment |
| Explorer / holders | Blockscout (si déployé) — API compatible Etherscan |
| Prix | DexScreener si la chaîne est indexée, sinon swaps ingérés |
| Sécurité | GoPlus si la chaîne est supportée, sinon simulation de vente (eth_call) |

Le coût d'ajout doit être faible grâce au `ChainAdapter` : l'adaptateur BNB est écrit dès V1 comme un **adaptateur EVM paramétrable** (adresses de factories/launchpads en config), Robinhood Chain devient essentiellement un fichier de configuration.

---

## 7. Interface

### 7.1 Principes UX

- **Un écran, un signal.** La vue principale montre *le* signal le plus prioritaire, pas un flux.
- **1–2 clics** pour toute action ; raccourcis clavier ; mêmes actions sur Telegram.
- **Pas de graphique inutile** : un sparkline depuis le first alert, c'est tout.
- **Couleur = sémantique** (vert rising/accumulating, ambre peaking, rouge fading/risk), jamais décorative.

### 7.2 Vue principale

```
┌──────────────────────────────────────────────────────────────────────────┐
│ NARRATIVE RADAR      ● SOL  ○ BNB  ○ RH          Timing: HOT 🔥   14:32 UTC│
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   $HATDOG  ·  solana  ·  7xKX…p3Qm  [copy]                 ENTER · HIGH   │
│   Narratif : « dogs with hats » — emerging · hype RISING ↑                │
│                                                                          │
│   ┌ état compact ─────────────────────────────────────────────────────┐  │
│   │ narrative:emerging hype:rising kol:curious early:strong           │  │
│   │ smart:accumulating cluster:independent curve:advanced             │  │
│   │ liquidity:adequate volume:surging holders:balanced risk:clean     │  │
│   └───────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│   First alert : 14:21:07 (il y a 11 min) @ $0.000412 · mcap $412k         │
│   Depuis alerte : +38.2 %   ATH +61.0 %   DD max −9.4 %   ▁▂▄▆█▇▆         │
│                                                                          │
│   Pourquoi : R-010 (hype rising + smart accumulating + risk clean)        │
│   Smart money : 4 wallets (AlphaWallets ×3, Cielo ×1) · 1 cluster indép.  │
│   KOL : @kolA (tier A, hit 41 %) 1ère mention il y a 3 min                │
│                                                                          │
│   [ ENTER 0.5 SOL ]  [ ENTER 1 SOL ]   [ Ignore ]   [ Snooze 15m ]        │
│        (E)               (Shift+E)         (I)           (S)              │
│                                                                          │
│   File d'attente : $FROGAI (bnb, medium) · $MOONCAT (sol, low)   (→)      │
├──────────────────────────────────────────────────────────────────────────┤
│ POSITIONS   $PEPEHAT +112 % [SCALE] [EXIT]   $CATJAM −12 % ⚠ EXIT suggéré │
└──────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Section « Outcomes / Performance since first alert »

**Définition du first alert** (immuable, append-only) :

| Champ | Définition |
|-------|------------|
| `first_alert_at` | Timestamp UTC (ms) où le token passe pour la 1ère fois en `ALERTED` (décision ≠ IGNORE) |
| `first_alert_block` | Slot/bloc correspondant |
| `ref_price_usd` / `ref_price_native` | Prix **au bloc de l'alerte** (dernier swap ≤ ce bloc), pas un prix recalculé plus tard |
| `ref_mcap`, `ref_liquidity` | Contexte au moment de l'alerte |
| `state_snapshot` | L'état compact complet + décision + version du rulebook |
| `first_seen_at` | Première détection (même sans alerte) → mesure de l'**avance** |

**Métriques calculées** :

- `gain_now = price_now / ref_price − 1`
- `gain_at_{5m,15m,1h,4h,24h,7d}` (snapshots planifiés)
- `ath_since_alert`, `time_to_ath`, `max_drawdown_since_alert`
- `gain_realistic` = gain en intégrant **slippage estimé** (d'après la liquidité au moment de l'alerte et une taille de référence) + frais ; c'est ce chiffre qui juge la qualité réelle
- `outcome` classé automatiquement à +24h : `winner` (ATH ≥ +100 % et gain_realistic à 1h > 0), `neutral`, `loser`, `rug` (liquidité −90 %)
- Si trade réel : PnL réalisé vs théorique (mesure la qualité d'exécution)

**Vue Outcomes** :

```
┌─ OUTCOMES ─ Performance since first alert ────── [24h] [7j] [30j] [Tout] ─┐
│ Filtres : chain [all▾]  confiance [all▾]  action [ENTER▾]  règle [all▾]   │
│                                                                          │
│  Alertes 214 · Hit rate (ATH ≥ +100 %) 31 % · Médiane gain@1h +8 %        │
│  Gain réaliste moyen@1h +14 % · Rugs 9 % · Avance moyenne X→alerte 4m12s  │
│                                                                          │
│ Token     Chain  Alerte     Conf  Action  @5m    @1h    ATH     Now   Res│
│ $HATDOG   sol    14:21      high  ENTER   +12%   —      +61%   +38%   ●  │
│ $FROGAI   bnb    13:02      med   ENTER   −4%    +22%   +140%  +65%   ✓  │
│ $RUGME    sol    12:47      low   IGNORE  +80%   −95%   +90%   −97%   ☠  │
│ $CATJAM   sol    11:15      high  ENTER   +30%   +210%  +340%  +95%   ✓  │
│ …                                                                        │
│                                                                          │
│ Par règle :  R-010 hit 44 % (n=61) · R-011 hit 29 % (n=38) · …            │
│ Par chaîne : sol hit 33 % · bnb hit 25 %                                  │
│ Par confiance : high 47 % · medium 30 % · low 18 %  ← calibration         │
└──────────────────────────────────────────────────────────────────────────┘
```

Points clés :
- Les **IGNORE** sont trackés aussi (colonne Action) → on voit les faux négatifs (ce qu'on a raté).
- La **calibration par confiance** doit être monotone (high > medium > low). Si elle ne l'est pas, c'est la première chose que le nightly loop doit signaler.
- Clic sur une ligne → détail : timeline (first_seen → alerte → ATH), état compact au moment de l'alerte, règles déclenchées, tweets et wallets impliqués.

### 7.4 Vue « Nightly Review » (gate humaine)

```
┌─ PROPOSITIONS DE RÈGLES — nuit du 26/09 ──────────────────────────────────┐
│ #P-114  Ajouter à R-010 : exiger volume ≠ climactic                        │
│   Motif : 7 ENTER high-confidence perdants avaient volume:climactic        │
│   Backtest 30j : hit rate 44 % → 49 % · alertes −8 % · faux négatifs +2    │
│   [ Approuver ]  [ Rejeter ]  [ Tester en shadow 7j ]                       │
└──────────────────────────────────────────────────────────────────────────┘
```

### 7.5 Telegram (canal secondaire)

Message compact = en-tête + état compact + gain depuis alerte + boutons inline `[Enter 0.5] [Ignore] [Snooze]`. Les boutons appellent la même API que le web (et en V1, l'exécution exige quand même la politique de risque côté executor).

---

## 7bis. Timing — heures chaudes

- **Calcul** : pour chaque chaîne, agréger volume DEX memecoin + nombre de launches + mentions X par **heure de la semaine** (168 buckets), sur 30 jours glissants → percentiles → `hot` (≥ p75), `warm` (p40–p75), `cold` (< p40).
- **Priors à utiliser en attendant les données (hypothèses à valider)** :
  - Solana : pic sur l'après-midi/soirée européenne ↔ ouverture US (≈ 13:00–22:00 UTC) en semaine.
  - BNB Chain : forte composante asiatique (≈ 01:00–10:00 UTC).
  - Robinhood Chain : inconnu → mesurer.
- Le Timing Agent influence surtout la **confiance** (et le sizing en V1), pas le veto.

---

## 7ter. Sécurité & architecture multi-wallet

```
 [UI / Telegram] ──(clic signé, session auth)──► [API brain]
                                                   │ ordre = {token, chain, action, sizeHint}
                                                   ▼
                                 ┌──────────── réseau isolé ────────────┐
                                 │ [Executor] ──► [Policy Engine]        │
                                 │                 │  limites :           │
                                 │                 │  - taille max/trade  │
                                 │                 │  - perte max/jour    │
                                 │                 │  - allowlist programmes/routers
                                 │                 │  - pas de transfert sortant hors vault
                                 │                 ▼                      │
                                 │            [Signer]  (Turnkey / Privy │
                                 │             server wallets / KMS)      │
                                 └────────────────────────────────────────┘
```

- **Aucun LLM, aucun agent, aucun prompt** dans le périmètre Executor/Signer. Le code executor n'importe pas `packages/agents` (règle vérifiée en CI par lint d'imports).
- Les ordres venant de l'UI sont des **intentions** ; l'executor reconstruit la transaction lui-même (pas de transaction pré-construite reçue de l'extérieur).
- **Multi-wallet** : par chaîne, N *hot wallets* de trading à solde plafonné + 1 *vault* (froid/multisig — Squads sur Solana, Safe sur EVM). Réapprovisionnement et sweep des profits vers le vault par règles, le vault n'est jamais contrôlé par l'executor.
- Rotation des hot wallets (limite le copy-trading de nos wallets par d'autres).
- Clés/API secrets dans un gestionnaire de secrets ; 2FA + allowlist IP sur l'UI ; journal d'audit append-only de chaque ordre.
- **MVP : aucune clé dans le système** (deep links uniquement) — le risque de sécurité est nul tant que l'edge n'est pas prouvé.

---

## 7quater. Loop d'amélioration nocturne

Pipeline (cron ≈ 03:00 UTC, heure creuse) :

1. **Collecte** : toutes les décisions des dernières 24h avec leur outcome (+24h), état compact, règles déclenchées.
2. **Sélection des erreurs à haute confiance** : `confidence = high ∧ outcome ∈ {loser, rug}` (faux positifs) et `action = IGNORE ∧ outcome = winner ∧ ATH ≥ +200 %` (faux négatifs coûteux).
3. **Analyse** (Claude Opus, contexte : échantillon d'erreurs + contre-exemples gagnants avec le même état + rulebook courant) → sortie **structurée** :
   ```ts
   RuleProposal { id, kind: "add"|"modify"|"remove"|"threshold",
                  diff, rationale, expectedEffect, evidenceAlertIds[] }
   ```
4. **Backtest automatique** de chaque proposition sur 30 jours d'états historiques (possible car les états compacts sont figés) → hit rate, nombre d'alertes, faux négatifs, calibration.
5. **Garde-fous automatiques** : rejet si < 20 exemples de support, si amélioration < seuil, ou si la règle ne fait que mémoriser des tokens précis (overfitting).
6. **Gate humaine** (UI §7.4) : Approuver / Rejeter / Shadow 7 jours. **Aucune règle n'entre en production sans clic humain.**
7. **Versionnage** : rulebook `vN+1` en Git (commit auto avec l'ID de proposition), rollback en 1 clic.
8. **Rapport** : calibration, dérive des KOL/wallets (déclassement proposé), nouveaux narratifs de la veille.

---

## 8. Risques et mitigations

| # | Risque | Impact | Mitigation |
|---|--------|--------|------------|
| 1 | **Accès X coûteux / instable** (pricing API, rate limits, CGU des tiers) | Couche narrative aveugle | Abstraire `XSource` ; Grok en MVP ; budget dédié au stream en V1 ; dégradation gracieuse (on-chain + smart money seuls) |
| 2 | **Hype artificielle** (bots, shills payés, raids) | Faux positifs | Pondération par qualité de compte, détection de copier-coller/coordination (Risk Cluster), hit-rate KOL mesuré en interne |
| 3 | **Rugs, bundles, honeypots, insiders** | Pertes sèches | Veto risk/insider non contournable, GoPlus/RugCheck, analyse de financement des holders, tracking des deployers récidivistes |
| 4 | **Prompt injection via tweets** (texte malveillant lu par un LLM) | Labels corrompus | Sorties LLM contraintes à des enums ; aucun outil/actions dans les agents ; aucun lien entre agents et executor ; valeurs hors schéma rejetées |
| 5 | **Fuite ou mauvais usage de clés** | Perte de fonds | Pas de clés au MVP ; signer externe ; policy engine ; hot wallets plafonnés ; vault multisig |
| 6 | **Biais du survivant / métriques flatteuses** | Illusion d'edge | Tracking de *toutes* les alertes y compris IGNORE ; prix de référence au bloc ; `gain_realistic` avec slippage |
| 7 | **Overfitting du nightly loop** | Dégradation silencieuse | Backtest out-of-sample, minimum de support, shadow mode, gate humaine, rollback |
| 8 | **Latence** (arriver après les snipers) | Edge nul sur le très court terme | Viser l'anticipation narrative (minutes/heures) plutôt que le sniping à la milliseconde ; mesurer la latence en continu |
| 9 | **Dépendance à des APIs non officielles** (GMGN, scrapers) | Casse soudaine | Toujours comme source secondaire ; registre de wallets interne ; suivi on-chain direct |
| 10 | **Qualité des listes smart money** (wallets connus = suivis par tous, ou devenus mauvais) | Signal dégradé | Wallet Scorer nocturne, déclassement auto proposé, préférence clusters indépendants |
| 11 | **Immaturité de Robinhood Chain** | Effort gaspillé | En V2 seulement, via adaptateur EVM paramétrable |
| 12 | **Coûts LLM** | Budget explosé | Pré-filtrage déterministe + Batch Filter avant Grok ; cache ; LLM seulement sur candidats ; budget/jour par agent avec coupe-circuit |
| 13 | **Réglementaire / fiscal** | Juridique | Outil d'aide à la décision personnel ; journal des trades exportable ; ne pas redistribuer de signaux sans cadre |
| 14 | **Surcharge cognitive** | Mauvaises décisions humaines | Un signal à la fois, file d'attente, snooze, limite d'alertes/heure |

---

## 9. Ordre de construction recommandé

Chaque étape produit quelque chose d'utilisable et de mesurable.

| Étape | Livrable | Durée indicative | Dépend de |
|-------|----------|------------------|-----------|
| **1** | Monorepo, `packages/core` (types Zod : événements, `CompactStateV1`, `Decision`), Postgres+Timescale, Redis, Docker Compose, migrations | 3 j | — |
| **2** | **SolanaAdapter** : launches pump.fun, swaps, migration PumpSwap/Raydium, snapshots token (Helius + DexScreener) | 1 sem | 1 |
| **3** | **Tracker** : first-alert ledger + snapshot scheduler + métriques (§7.3). *Construit tôt : c'est lui qui dira si tout le reste vaut quelque chose* | 4 j | 2 |
| **4** | Market Feature Builder + Holder Analyzer + Security Checker → **Adjectivizer** (slots on-chain) | 4 j | 2 |
| **5** | **Rulebook engine** déterministe + state machine + alertes « on-chain only » → premières alertes trackées | 3 j | 3, 4 |
| **6** | **UI minimale** (signal card + Outcomes) + bot Telegram + deep links | 1 sem | 5 |
| **7** | **X Ingestor** (Grok) + Narrative Tracker + Hype Velocity + KOL Watcher + Entity Resolver → slots narratifs | 1,5 sem | 5 |
| **8** | Smart money v0 : import AlphaWallets → registre, webhooks Helius sur ces wallets → slot `smart` | 4 j | 5 |
| **9** | Timing (priors), rapport nocturne lecture seule → **fin du MVP**, période d'observation de 2–3 semaines | 3 j | 7, 8 |
| **10** | Early Signal, Smart Money Cross-Checker complet, Cluster Detector, Risk Cluster | 2 sem | MVP |
| **11** | Batch Filter (Quail) + nightly loop complet (propositions → backtest → gate humaine) | 1,5 sem | 10 |
| **12** | **Adaptateur EVM** paramétrable + config **BNB** (four.meme, PancakeSwap, GoPlus) | 1,5 sem | MVP |
| **13** | Jev/Kev en mode shadow | 1 sem | 11 |
| **14** | Executor isolé + signer + policy engine + multi-wallet → ENTER/EXIT en 1 clic → **fin V1** | 2 sem | preuve d'edge en Outcomes |
| **15** | V2 : Yellowstone gRPC, Robinhood Chain (config EVM), cross-chain narratives, auto SCALE/EXIT bornés, scoring KOL avancé | continu | V1 |

**Règle de passage** : on ne construit l'exécution (étape 14) que si la section Outcomes montre un `gain_realistic` positif et une calibration monotone sur ≥ 200 alertes.

---

## Annexe A — Modèle de données

```sql
-- Référentiel
create table tokens (
  id text primary key,                 -- "solana:<mint>" | "bnb:0x…" | "robinhood:0x…"
  chain text not null check (chain in ('solana','bnb','robinhood')),
  address text not null,
  symbol text, name text,
  deployer text,
  launchpad text,                      -- pumpfun | letsbonk | fourmeme | …
  created_at timestamptz not null,
  migrated_at timestamptz
);

create table narratives (
  id uuid primary key, label text, keywords text[],
  first_seen_at timestamptz, stage text
);

create table token_narratives (token_id text references tokens, narrative_id uuid references narratives,
  link_confidence text, linked_at timestamptz, primary key (token_id, narrative_id));

-- Flux (hypertables Timescale)
create table tweets (id text primary key, author text, author_tier text, text text,
  created_at timestamptz, ingested_at timestamptz, token_refs text[], narrative_id uuid);
create table swaps (token_id text, ts timestamptz, block bigint, wallet text, side text,
  amount_native numeric, amount_usd numeric, price_usd numeric);          -- hypertable
create table token_features (token_id text, ts timestamptz, features jsonb); -- hypertable

-- Smart money
create table wallets (address text, chain text, source text, tags text[], score numeric,
  cluster_id uuid, active boolean, primary key (chain, address));

-- Décision & tracking
create table decisions (id uuid primary key, token_id text, ts timestamptz,
  state jsonb not null, state_text text not null,         -- état compact figé
  action text, confidence text, rule_ids text[], rulebook_version text, engine text);

create table alerts (                    -- append-only : pas d'UPDATE applicatif
  token_id text primary key references tokens,
  first_seen_at timestamptz, first_alert_at timestamptz not null, first_alert_block bigint,
  decision_id uuid references decisions,
  ref_price_usd numeric not null, ref_price_native numeric, ref_mcap numeric, ref_liquidity numeric
);

create table alert_snapshots (token_id text, horizon text, ts timestamptz, price_usd numeric,
  gain numeric, gain_realistic numeric, primary key (token_id, horizon));

create table alert_outcomes (token_id text primary key, ath_gain numeric, time_to_ath interval,
  max_drawdown numeric, outcome text, evaluated_at timestamptz);

create table trades (id uuid primary key, token_id text, wallet text, side text, ts timestamptz,
  amount_native numeric, price_usd numeric, tx text, source text);        -- manuel (MVP) ou executor

-- Amélioration
create table rule_proposals (id text primary key, created_at timestamptz, kind text, diff text,
  rationale text, backtest jsonb, status text, reviewed_by text, reviewed_at timestamptz);
```

## Annexe B — Points à vérifier avant de coder

Ces éléments conditionnent des choix ci-dessus et doivent être validés (docs officielles, tests d'API) :

1. **AlphaWallets.fun** : existe-t-il une API ou un export ? Format, fréquence, CGU d'usage automatisé. Sinon : import manuel périodique.
2. **Jev (TypeSafe) / Kev** : interface exacte (entrée texte vs objet, latence, auto-hébergement). L'interface `DecisionEngine` permet d'adapter sans toucher au reste.
3. **Quail (AI-SQL)** : mode de déploiement, connecteurs (Postgres ?), coût au volume.
4. **xAI Grok API** : quotas et fraîcheur réelle de la recherche X (latence tweet → résultat), coût par requête.
5. **X API officielle** : palier nécessaire pour un filtered stream et coût mensuel.
6. **Robinhood Chain** : statut mainnet, DEX et launchpads déployés, support RPC/indexeurs (DexScreener, GoPlus, Blockscout).
7. **Launchpads actifs** au moment du build (le paysage Solana/BNB bouge vite : pump.fun, letsbonk, Meteora DBC, four.meme…) — garder la liste en config.
8. **GMGN / Cielo** : disponibilité API et limites.
