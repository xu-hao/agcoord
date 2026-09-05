# Asset sources

[`agcoord-gourd-mascot.png`](agcoord-gourd-mascot.png) is the selected 1254 × 1254 transparent
RGBA gourd mascot used by the root README. It depicts a double-lobed golden bottle gourd with
a curled green stem and one leaf in a restrained naturalist botanical style. Keep
that canonical path stable so README and package-description URLs do not change.

## Generation record

- Date: 2026-08-30
- Tool: OpenAI built-in image generation, edit mode
- Reference: the preceding mascot at the same canonical path
- SHA-256: `290b63a00c6e291741700f193a8b8fece2cf6654a8d74fdad2f7ffd18c44e6fd`
- Prompt (verbatim):

```text
Use case: logo-brand
Asset type: open-source developer tool mascot/emblem, transparent PNG
Primary request: Redesign the referenced yellow gourd as a sophisticated naturalist botanical mark with absolutely no face or anthropomorphic features.
Input image: identity reference; preserve the recognizable double-lobed bottle-gourd silhouette, curled green stem, single leaf, and warm golden-yellow body.
Style/medium: refined vintage botanical plate meets contemporary screen print; restrained etched contour and subtle stippled texture, crisp enough for a software README.
Composition: centered upright full gourd, leaf balanced to the upper right, generous transparent margin, square canvas.
Palette: ochre gold, muted amber, deep forest and olive green, dark umber linework.
Constraints: no eyes, mouth, facial marks, arms, hands, legs, feet, pose, blush, text, letters, badge border, scene, drop shadow, watermark, or extra objects; genuinely transparent background; legible at 240 pixels.
```

Any later replacement must retain its editable source or generation record,
license/provenance, and accessible description here rather than silently overwriting the
published identity.

## Terminal UI screenshot

[`agcoord-tui.svg`](agcoord-tui.svg) is the screenshot of `agc tui` that the root README shows.
It is an SVG that Textual exported from the real application, so the text is selectable and it
renders at any size; it shows a 100 × 24 terminal with two repositories, a landing in its
gating phase, a running check, two queued jobs, recent history, the detail pane for the selected
row, and the capacity footer. Keep the canonical path stable so the README URL does not change.

### Generation record

- Date: 2026-09-05
- Tool: Textual 8.2.8, `App.save_screenshot` inside `App.run_test(size=(100, 24))`, driving
  `agcoord.tui.build_app` from AGCoord 0.6.3 with a fixed in-memory snapshot in the durable row
  shape (no broker, credentials, or network); one `down` key press selects the second row
- Content: repositories `github.com/acme/api` and `github.com/acme/web`; agents `claude-a`,
  `claude-c`, and `codex-b`; rows `land-8c1f2a9d4e07` (gating), `check-5d2e77b1c3a9` (running),
  `full-0a3b6c9d2e5f` and `check-e4f5a6b7c8d9` (queued), and four terminal rows including a
  `stale-main` handback; capacities `jobs=4`, `cpu=8`, `memory=24 GiB`, `tmpfs=8 GiB`
- SHA-256: `c6cdce6a580adbea2b62e2ef63c019e2762985c1bf11d367f730d706151120a4`
- Regenerate by re-exporting from the current `agc tui` with the same snapshot whenever the
  table layout changes, and update this record

Any later replacement must retain its editable source or generation record,
license/provenance, and accessible description here rather than silently overwriting the
published identity.
