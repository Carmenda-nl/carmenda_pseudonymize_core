# Release Drafter Setup - Quick Start Guide

## 📦 Wat is geïnstalleerd?

1. **Release Drafter Workflow** (`.github/workflows/release-drafter.yml`)
   - Draait bij elke PR naar `develop`
   - Update automatisch een draft release

2. **Release Drafter Config** (`.github/release-drafter.yml`)
   - Keep a Changelog format
   - Autolabeler op basis van PR titels
   - Categorieën: Added, Changed, Fixed, Security, etc.

3. **Beta Release Integration** (`.github/workflows/beta-release.yml`)
   - Gebruikt draft release notes automatisch bij tag push
   - Fallback naar simpele notes als geen draft beschikbaar

## 🚀 Hoe het werkt

### 1. Maak een PR met goede titel

De PR titel bepaalt automatisch het label en de changelog sectie:

```bash
# Maak een feature branch
git checkout develop
git pull origin develop
git checkout -b feature/add-csv-export

# ... ontwikkel de feature ...
git add .
git commit -m "Add CSV export functionality"
git push origin feature/add-csv-export
```

### 2. Maak PR via GitHub

**Via GitHub Web UI:**
1. Ga naar je repository op GitHub
2. Klik "Pull requests" → "New pull request"
3. Base: `develop` ← Compare: `feature/add-csv-export`
4. Titel: `feat: Add CSV export functionality`
5. Beschrijving invullen
6. Create pull request

**✅ Automatisch:**
- Label `feature` wordt toegevoegd (door autolabeler)
- Draft release wordt bijgewerkt met deze PR onder "### Added"

**Alternatief via command line (zonder gh CLI):**
```bash
# Push branch
git push origin feature/add-csv-export

# Open de URL die git geeft in je browser
# Of ga naar: https://github.com/Carmenda-nl/carmenda_pseudonymize_core/compare/develop...feature/add-csv-export
```

### 3. Check Draft Release

1. Ga naar: https://github.com/Carmenda-nl/carmenda_pseudonymize_core/releases
2. Je ziet een "Draft" release met:
   - Versienummer (automatisch berekend)
   - Keep a Changelog formaat
   - Jouw PR onder "### Added"

## 📝 PR Titel Conventies

Release Drafter gebruikt je PR titel om automatisch te labelen:

| Titel Prefix | Auto Label | Changelog Sectie |
|--------------|------------|------------------|
| `feat:` of `feature:` of `add:` | `feature` | **Added** |
| `fix:` of `bug:` of `bugfix:` | `bug` | **Fixed** |
| `update:` of `upgrade:` of `bump:` | `update` | **Changed** |
| `refactor:` | `refactor` | **Changed** |
| `change:` of `modify:` | `change` | **Changed** |
| `enhance:` of `improve:` | `enhancement` | **Added** |
| Bevat `security` of `CVE` | `security` | **Security** |
| `remove:` of `delete:` | `removal` | **Removed** |
| `docs:` of `doc:` | `documentation` | (uitgesloten) |
| `chore:` | `chore` | (uitgesloten) |

## 💡 Voorbeelden

### Feature toevoegen via GitHub UI
1. Push branch: `git push origin feature/websocket-notifications`
2. Ga naar GitHub → New Pull Request
3. Titel: `feat: Add real-time WebSocket notifications`
4. Beschrijving: "Implements WebSocket for push notifications"
5. Create PR
**→ Label: `feature` → Sectie: ### Added**

### Bug fixen
1. Push branch: `git push origin bugfix/encoding-issue`
2. GitHub → New Pull Request
3. Titel: `fix: Resolve UTF-8 encoding in CSV parser`
4. Beschrijving: "Fixes #123"
5. Create PR
**→ Label: `bug` → Sectie: ### Fixed**

### Dependency updaten
1. Push branch: `git push origin update/polars-v2`
2. GitHub → New Pull Request
3. Titel: `update: Upgrade Polars to v2.0`
4. Beschrijving: "Updates polars dependency"
5. Create PR
**→ Label: `update` → Sectie: ### Changed**

### Refactoring
1. Push branch: `git push origin refactor/deduplication`
2. GitHub → New Pull Request
3. Titel: `refactor: Simplify deduplication logic`
4. Beschrijving: "Refactored for better readability"
5. Create PR
**→ Label: `refactor` → Sectie: ### Changed**

## 🔄 Workflow van PR naar Release

1. **PR gemaakt** → Release Drafter update draft
2. **PR gemerged** → Draft release blijft staan
3. **Meer PRs gemerged** → Draft wordt steeds uitgebreider
4. **Beta tag pushen** (`v1.2.11-beta`) → Draft notes worden gebruikt voor release

```bash
# Wanneer klaar voor release:
git tag v1.2.11-beta
git push origin v1.2.11-beta

# Beta-release workflow:
# ✅ Haalt draft release notes op
# ✅ Maakt beta release met die notes (Keep a Changelog format!)
# ✅ Bouwt backend
# ✅ Upload naar release
```

## 🎯 Beta Release Notes Voorbeeld

Na een paar PRs ziet je beta release er zo uit:

```markdown
## [1.2.11-beta] - 2026-01-22

### Added

- feat: Add real-time WebSocket notifications (#125)
- feat: Add CSV export functionality (#126)

### Changed

- update: Upgrade Polars to v2.0 (#127)
- refactor: Simplify deduplication logic (#128)

### Fixed

- fix: Resolve UTF-8 encoding in CSV parser (#129)
- fix: Handle null values in report column (#130)
```

Perfect Keep a Changelog format, automatisch gegenereerd! 🎉

## ⚙️ Configuratie Aanpassen

### Meer title patterns toevoegen

Edit `.github/release-drafter.yml`:

```yaml
autolabeler:
  - label: 'feature'
    title:
      - '/^feat/i'
      - '/^feature/i'
      - '/^add/i'
      - '/new feature/i'  # ← Voeg toe
```

### Andere categorieën toevoegen

```yaml
categories:
  - title: '### Breaking Changes'
    labels:
      - 'breaking-change'
  - title: '### Performance'
    labels:
      - 'performance'
```

## 🔍 Troubleshooting

### Draft release wordt niet aangemaakt

**Check:**
1. Is de PR naar `develop` branch?
2. Heeft GitHub Actions rechten? (Settings → Actions → General → Workflow permissions)
3. Check workflow runs: Actions tab → Release Drafter

### Label wordt niet automatisch toegevoegd

**Check:**
1. Komt PR titel overeen met pattern? (case-insensitive)
2. Voorbeeld: `feat:` werkt, `FEAT:` werkt, `Feat:` werkt
3. Edit PR titel en label wordt bijgewerkt

### Verkeerde categorie in release notes

**Oplossing:**
1. Wijzig label handmatig in GitHub UI (rechterkant van PR)
2. Release Drafter update automatisch de draft

## 📚 Meer Informatie

- Release Drafter Docs: https://github.com/release-drafter/release-drafter
- Keep a Changelog: https://keepachangelog.com/

## ✅ Checklist

- [x] Release Drafter workflow geïnstalleerd
- [x] Release Drafter config met Keep a Changelog format
- [x] GitHub labels aangemaakt
- [x] Beta-release integreert draft notes
- [ ] Test PR gemaakt met `feat:` prefix
- [ ] Draft release verschijnt op Releases pagina
- [ ] Klaar voor productie gebruik!
