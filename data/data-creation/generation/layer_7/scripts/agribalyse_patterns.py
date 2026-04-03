"""
Search patterns for Agribalyse gap-fill.

Each pattern is a SQL LIKE-style string where % is a wildcard.
Patterns are tried in order; first match with valid water data wins.
Patterns are kept conservative to avoid false positives (e.g.,
"wire drawing" matching textile "drawing").
"""

# -- Material patterns --
MATERIAL_PATTERNS = {
    "chicken feathers, at slaughterhouse": [
        "%chicken%feather%",
        "%poultry%feather%",
    ],
    "duck feathers (fattened), at slaughterhouse": [
        "%duck%feather%",
        "%poultry%feather%",
    ],
    "duck feathers, at slaughterhouse": [
        "%duck%feather%",
        "%poultry%feather%",
    ],
    "fibre, cotton, organic": [
        "%seed-cotton%organic%",
        "%organic%cotton%production%",
    ],
    "silky fibre": [
        "%kapok%",
        "%silk%fibre%",
        "%cocoon%production%",
    ],
    "beef hides, at slaughterhouse": [
        "%slaughter%beef%hide%",
        "%hide%beef%",
        "%slaughter%hide%",
    ],
    "cowhide, from beef, at slaughterhouse": [
        "%slaughter%beef%hide%",
        "%cowhide%",
        "%slaughter%hide%",
    ],
    "lamb hide, at slaughterhouse": [
        "%lamb%hide%",
        "%sheep%hide%",
        "%slaughter%lamb%",
    ],
    "veal hide, at slaughterhouse": [
        "%veal%hide%",
        "%calf%hide%",
        "%slaughter%veal%",
    ],
    "cork slab": [
        "%cork%slab%",
        "%cork%raw%",
        "%cork%forestry%",
        "%cork%production%",
    ],
    "cork, raw": [
        "%cork, raw%",
        "%cork%raw%",
        "%cork%forestry%",
    ],
    "polylactic acid, granulate": [
        "%polylactide%granulate%",
        "%polylactide%production%",
        "%polylactic%",
    ],
    "textile, jute": [
        "%textile, jute%",
        "%textile%jute%weaving%",
        "%jute%textile%",
    ],
    "textile, jute, woven": [
        "%textile, jute%",
        "%textile%jute%weaving%",
    ],
    "textile, kenaf": [
        "%textile%kenaf%",
        "%kenaf%textile%",
    ],
    "textile, silk": [
        "%textile%silk%",
        "%silk%textile%",
        "%silk%weaving%",
    ],
}

# -- Process patterns --
PROCESS_PATTERNS = {
    "ginning": [
        "%ginning%",
        "%cotton%ginning%",
    ],
    "scutching": [
        "%scutching%",
    ],
    "decortication": [
        "%decortication%",
    ],
    "degumming": [
        "%degumming%",
    ],
    "scouring": [
        "%scouring%",
    ],
    "carding": [
        "%carding%",
    ],
    "combing": [
        "%combing%",
    ],
    "texturing": [
        "%texturing%",
        "%texturis%",
    ],
    "knitting": [
        "%knitting%",
        "%textile%knit%",
    ],
    "printing": [
        "%printing%textile%",
        "%textile%printing%",
    ],
    "raising": [
        "%raising%textile%",
    ],
    "coating": [
        "%coating%textile%",
        "%textile%coating%",
    ],
    "laminating": [
        "%laminating%without%solvent%",
        "%laminating%with%solvent%",
        "%laminating%",
    ],
    "waterproofing": [
        "%waterproofing%",
        "%water-repellent%",
    ],
    "flame retardant treatment": [
        "%flame%retardant%treatment%",
        "%flame%retardant%textile%",
    ],
    "antimicrobial treatment": [
        "%antimicrobial%",
    ],
    "softening": [
        "%softening%",
    ],
    "drawing": [
        "%drawing%fibre%",
        "%fibre%drawing%",
    ],
    "heat setting": [
        "%heat%setting%",
    ],
    "cutting": [
        "%cutting%textile%",
        "%cutting%fabric%",
    ],
    "beamhouse": [
        "%beamhouse%",
    ],
    "chrome_tanning": [
        "%chrome%tanning%",
        "%tanning%",
    ],
    "retanning_dyeing": [
        "%retanning%",
    ],
    "leather_finishing": [
        "%leather%finishing%",
        "%finishing%leather%",
    ],
    "foam_cutting": [
        "%foam%cutting%",
    ],
    "foam_lamination": [
        "%foam%lamination%",
        "%laminating%foam%",
    ],
    "vulcanisation": [
        "%vulcanis%",
        "%vulcaniz%",
    ],
    "rubber_moulding": [
        "%rubber%mould%",
        "%rubber%mold%",
    ],
    "calendering_rubber": [
        "%calender%rubber%",
        "%rubber%calender%",
        "%calendering%rigid%",
        "%calendering%",
    ],
    "washing_sanitising": [
        "%washing%feather%",
        "%washing%down%",
    ],
    "sorting_grading": [
        "%sorting%feather%",
    ],
    "drying": [
        "%sun drying%",
        "%drying%processing%",
        "%drying%grain%",
        "%drying%",
    ],
    "cork_boiling": [
        "%boiling%cork%",
        "%cork%boiling%",
        "%cork%processing%",
    ],
    "cork_pressing": [
        "%pressing%cork%",
        "%cork%pressing%",
    ],
    "cork_grinding": [
        "%grinding%cork%",
        "%cork%grinding%",
    ],
}
