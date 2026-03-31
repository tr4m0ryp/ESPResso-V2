"""
EcoInvent search patterns for water consumption extraction.

Each dict maps item_name -> list of SQL LIKE patterns to try in
priority order. Patterns use % as wildcard.

Split across two files for the 300-line limit:
  - ecoinvent_patterns.py: materials (this file)
  - ecoinvent_patterns_proc.py: processing steps + packaging
"""

from ecoinvent_patterns_proc import PACKAGING_PATTERNS, PROCESS_PATTERNS

MATERIAL_PATTERNS = {
    "chicken feathers, at slaughterhouse": [
        "%chicken%feather%", "%feather%chicken%"],
    "duck feathers (fattened), at slaughterhouse": [
        "%duck%feather%", "%feather%duck%"],
    "duck feathers, at slaughterhouse": [
        "%duck%feather%", "%feather%duck%"],
    "cellulose fibre": [
        "%fibre production, viscose%fibre, viscose%",
        "%dissolving pulp%production%",
        "%viscose%fibre%"],
    "coconut fibre, at storehouse": [
        "%coconut production%dehusked%coconut, dehusked%",
        "%coconut%fibre%", "%coir%production%"],
    "cottonized fibre, hemp": [
        "%hemp stem production%dew-retted%hemp stem%",
        "%hemp%fibre%production%", "%hemp%retting%fibre%",
        "%cottoniz%hemp%"],
    "decorticated fibre, hemp": [
        "%hemp stem production%dew-retted%hemp stem%",
        "%hemp%decortication%", "%decorticated%hemp%",
        "%hemp%fibre%"],
    "fibre, cotton": [
        "%seed-cotton production%conventional%seed-cotton%",
        "%seed-cotton production%conventional%",
        "%fibre production%cotton%ginning%fibre, cotton%"],
    "fibre, cotton, organic": [
        "%seed-cotton production%organic%seed-cotton, organic%",
        "%seed-cotton production%organic%",
        "%fibre production%cotton%organic%"],
    "fibre, flax": [
        "%fibre production, flax%retting%fibre, flax%",
        "%fibre production%flax%fibre, flax%"],
    "fibre, flax, long, scutched": [
        "%fibre production, flax%retting%fibre, flax%",
        "%scutching%flax%long%"],
    "fibre, flax, short, scutched": [
        "%fibre production, flax%retting%fibre, flax%",
        "%scutching%flax%short%"],
    "fibre, jute": [
        "%fibre production, jute%retting%fibre, jute%",
        "%fibre production%jute%fibre, jute%"],
    "fibre, kenaf": [
        "%fibre production, kenaf%retting%fibre, kenaf%",
        "%fibre production%kenaf%fibre, kenaf%"],
    "fibre, polyester": [
        "%polyester fibre production%fibre, polyester%",
        "%polyester%fibre%production%"],
    "fibre, silk, short": [
        "%cocoon production%silkworm%cocoons%",
        "%silk%fibre%"],
    "fibre, viscose": [
        "%fibre production, viscose%fibre, viscose%",
        "%fibre production%viscose%"],
    "flax straw, retted, at farm gate": [
        "%fibre production, flax%retting%",
        "%flax%retting%"],
    "hemp fibre, raw, at farm gate": [
        "%hemp stem production%dew-retted%hemp stem%",
        "%hemp%fibre%production%", "%hemp%retting%"],
    "hemp straw, retted, at farm gate": [
        "%hemp stem production%dew-retted%hemp stem%",
        "%hemp%retting%", "%hemp%fibre%"],
    "seed cotton, conventional": [
        "%seed-cotton production%conventional%seed-cotton%",
        "%seed-cotton production, conventional%"],
    "seed cotton, conventional, global average": [
        "%seed-cotton production%conventional%seed-cotton%",
        "%seed-cotton production, conventional%"],
    "sheep fleece in the grease": [
        "%sheep%fleece%", "%wool production%",
        "%sheep%production%wool%"],
    "silky fibre": [
        "%kapok%fibre%", "%kapok%production%"],
    "wool, Roquefort dairy sheep, at farm gate": [
        "%wool%sheep%production%", "%sheep%wool%"],
    "wool, conventional, at farm gate": [
        "%wool%sheep%production%", "%sheep%wool%"],
    "wool, organic (system 2), at farm gate": [
        "%wool%sheep%organic%", "%sheep%wool%organic%",
        "%sheep production%wool%", "%sheep%fleece%"],
    "wool, organic, at farm gate": [
        "%wool%sheep%organic%", "%sheep%wool%organic%",
        "%sheep production%wool%", "%sheep%fleece%"],
    "cotton string": [
        "%seed-cotton production%conventional%",
        "%cotton%string%"],
    "rope, coconut fibre": [
        "%coconut production%dehusked%coconut, dehusked%",
        "%coconut%fibre%", "%coir%production%"],
    "polystyrene foam slab": [
        "%polystyrene production, expandable%polystyrene, expandable%",
        "%polystyrene production%general purpose%polystyrene%",
        "%polystyrene foam slab production%polystyrene foam slab%"],
    "polyurethane, flexible foam": [
        "%polyurethane production%flexible foam%polyurethane, flexible foam%",
        "%polyurethane%flexible foam%"],
    "polyurethane, flexible foam, flame retardant": [
        "%polyurethane production%flexible foam%polyurethane, flexible foam%",
        "%polyurethane%flexible foam%"],
    "polyurethane, rigid foam": [
        "%polyurethane production%rigid foam%polyurethane, rigid foam%",
        "%polyurethane%rigid foam%"],
    "beef hides, at slaughterhouse": [
        "%cattle%hide%production%", "%beef%hide%",
        "%hide production%cattle%"],
    "cowhide, from beef, at slaughterhouse": [
        "%cattle%hide%production%", "%cowhide%",
        "%hide production%"],
    "lamb hide, at slaughterhouse": [
        "%lamb%hide%", "%sheep%hide%production%"],
    "veal hide, at slaughterhouse": [
        "%veal%hide%", "%calf%hide%production%"],
    "aluminium, primary, ingot": [
        "%aluminium production, primary, ingot%aluminium, primary, ingot%",
        "%aluminium%primary%ingot%production%"],
    "aluminium, wrought alloy": [
        "%aluminium scrap%remelter%aluminium, wrought alloy%",
        "%aluminium%wrought alloy%"],
    "brass": [
        "%brass production%brass%", "%contouring, brass%"],
    "copper, cathode": [
        "%copper production%cathode%solvent extraction%copper, cathode%",
        "%electrolytic refining%copper, cathode%",
        "%copper%cathode%production%"],
    "nickel, class 1": [
        "%nickel, class 1%", "%nickel production%class 1%"],
    "steel, chromium steel 18/8": [
        "%steel production%chromium steel 18/8%steel, chromium steel 18/8%",
        "%chromium steel 18/8%production%"],
    "steel, low-alloyed": [
        "%steel production%low-alloyed%steel, low-alloyed%",
        "%steel%low-alloyed%production%"],
    "zinc": [
        "%primary zinc production%zinc%",
        "%zinc production%zinc%"],
    "cork slab": [
        "%cork slab production%cork slab%",
        "%cork%slab%production%"],
    "cork, raw": [
        "%cork%production%", "%cork%harvesting%"],
    "ethylene vinyl acetate copolymer": [
        "%ethylene vinyl acetate copolymer production%ethylene vinyl acetate%",
        "%ethylene vinyl acetate%production%"],
    "nylon 6": [
        "%nylon 6 production%nylon 6%",
        "%nylon 6 production |%"],
    "nylon 6-6": [
        "%nylon 6-6 production%nylon 6-6%",
        "%nylon 6-6 production |%"],
    "polyethylene terephthalate, granulate, amorphous": [
        "%polyethylene terephthalate production%amorphous%",
        "%polyethylene terephthalate%amorphous%production%"],
    "polyethylene terephthalate, granulate, bottle grade": [
        "%polyethylene terephthalate production%granulate, bottle grade%",
        "%polyethylene terephthalate production%bottle%",
        "%polyethylene terephthalate%bottle%production%"],
    "polylactic acid, granulate": [
        "%polylactic acid%production%",
        "%polylactide%production%"],
    "polypropylene, granulate": [
        "%polypropylene production%granulate%polypropylene, granulate%",
        "%polypropylene%granulate%production%"],
    "latex": [
        "%latex production%latex%", "%natural rubber%latex%"],
    "seal, natural rubber based": [
        "%latex production%latex%", "%natural rubber%production%"],
    "styrene butadiene rubber, emulsion polymerised": [
        "%styrene butadiene rubber production%emulsion%styrene butadiene rubber%",
        "%butadiene rubber%emulsion%"],
    "styrene butadiene rubber, solution polymerised": [
        "%styrene butadiene rubber production%solution%styrene butadiene rubber%",
        "%butadiene rubber%solution%"],
    "synthetic rubber": [
        "%synthetic rubber production%synthetic rubber%",
        "%synthetic rubber%production%"],
    "textile, jute": [
        "%textile production%jute%textile, jute%",
        "%jute%textile%production%"],
    "textile, jute, woven": [
        "%textile production%jute%textile, jute%",
        "%jute%textile%woven%"],
    "textile, kenaf": [
        "%textile production%kenaf%textile, kenaf%",
        "%kenaf%textile%production%"],
    "textile, knit cotton": [
        "%textile production%cotton%knit%textile, knit cotton%",
        "%finishing, textile, knit cotton%",
        "%knit%cotton%textile%production%"],
    "textile, nonwoven polyester": [
        "%textile production%nonwoven polyester%",
        "%nonwoven%polyester%production%"],
    "textile, nonwoven polypropylene": [
        "%textile production%nonwoven polypropylene%",
        "%nonwoven%polypropylene%production%"],
    "textile, silk": [
        "%textile production%silk%textile, silk%",
        "%silk%textile%production%"],
    "textile, woven cotton": [
        "%textile production%cotton%weaving%textile, woven cotton%",
        "%textile production%cotton%woven%"],
    "reeled raw silk hank": [
        "%silk reeling%reeled%silk%",
        "%cocoon production%silkworm%cocoons%"],
    "yarn, cotton": [
        "%yarn production, cotton%yarn, cotton%",
        "%spinning%cotton%yarn, cotton%"],
    "yarn, jute": [
        "%yarn production%jute%yarn, jute%",
        "%jute%yarn%production%"],
    "yarn, jute, global market": [
        "%yarn production%jute%yarn, jute%",
        "%jute%yarn%production%"],
    "yarn, kenaf": [
        "%yarn production%kenaf%yarn, kenaf%",
        "%kenaf%yarn%production%"],
    "yarn, silk": [
        "%silk reeling%silk%", "%yarn%silk%production%",
        "%cocoon production%silkworm%"],
}

__all__ = [
    "MATERIAL_PATTERNS", "PROCESS_PATTERNS", "PACKAGING_PATTERNS",
]
