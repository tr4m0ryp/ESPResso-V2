"""
EcoInvent search patterns for processing steps and packaging.

See ecoinvent_patterns.py for material patterns and documentation.
"""

PROCESS_PATTERNS = {
    "ginning": [
        "%ginning%fibre%cotton%", "%cotton%ginning%",
        "%fibre production%cotton%ginning%"],
    "retting": [
        "%retting%fibre%", "%fibre production%retting%",
        "%retting%"],
    "scutching": [
        "%scutching%flax%", "%flax%scutching%"],
    "decortication": [
        "%decortication%hemp%", "%hemp%decortication%"],
    "degumming": [
        "%degumming%silk%", "%degumming%"],
    "scouring": [
        "%scouring%wool%", "%scouring%cotton%",
        "%scouring%"],
    "carding": [
        "%carding%fibre%", "%carding%wool%",
        "%carding%"],
    "combing": [
        "%combing%fibre%", "%combing%wool%",
        "%combing%"],
    "spinning": [
        "%yarn production%cotton%spinning%yarn, cotton%",
        "%spinning%yarn%", "%ring spinning%",
        "%open end spinning%"],
    "texturing": [
        "%texturing%fibre%", "%texturing%polyester%",
        "%texturing%"],
    "weaving": [
        "%textile production%air jet loom weaving%",
        "%textile production%weaving%",
        "%weaving%cotton%", "%weaving%textile%"],
    "knitting": [
        "%textile production%knitting%",
        "%knitting%cotton%", "%knitting%"],
    "nonwoven production": [
        "%nonwoven%production%", "%textile production%nonwoven%"],
    "bleaching": [
        "%bleaching%textile%", "%bleaching, textile%",
        "%bleaching%"],
    "batch dyeing": [
        "%batch dyeing%fibre%cotton%",
        "%batch dyeing%woven%cotton%",
        "%batch dyeing%"],
    "continuous dyeing": [
        "%continuous dyeing%fibre%cotton%",
        "%continuous dyeing%"],
    "printing": [
        "%printing%textile%", "%textile%printing%"],
    "mercerizing": [
        "%mercerizing%", "%mercerising%"],
    "sanforizing": [
        "%sanforizing%", "%sanforising%",
        "%pre-shrinking%"],
    "finishing": [
        "%finishing%textile%woven cotton%",
        "%finishing%textile%knit cotton%",
        "%finishing, textile%", "%finishing%textile%"],
    "calendering": [
        "%calendering%textile%", "%calendering%"],
    "raising": [
        "%raising%textile%", "%raising%"],
    "coating": [
        "%coating%textile%", "%textile%coating%"],
    "laminating": [
        "%laminating%textile%", "%lamination%"],
    "waterproofing": [
        "%waterproofing%", "%water-repellent%"],
    "flame retardant treatment": [
        "%flame retardant%", "%fire retardant%"],
    "antimicrobial treatment": [
        "%antimicrobial%", "%antibacterial%"],
    "softening": [
        "%softening%textile%", "%softening%"],
    "extrusion": [
        "%extrusion%fibre%", "%melt spinning%",
        "%polyester fibre production%"],
    "drawing": [
        "%drawing%fibre%", "%fibre%drawing%"],
    "heat setting": [
        "%heat setting%", "%heat-setting%"],
    "cutting": [
        "%cutting%textile%", "%cutting%fabric%"],
    "stamping": [
        "%stamping%metal%", "%stamping%steel%",
        "%stamping%", "%sheet rolling%"],
    "machining": [
        "%chromium steel milling, average%",
        "%machining%metal%", "%milling%steel%",
        "%milling%chromium steel%"],
    "electroplating": [
        "%electroplating%", "%chrome plating%",
        "%zinc coating%electrolytic%",
        "%anodising%aluminium%"],
    "casting": [
        "%casting%aluminium%lost-wax%",
        "%casting%steel%lost-wax%",
        "%casting%metal%", "%die casting%"],
    "beamhouse": [
        "%beamhouse%", "%leather%liming%",
        "%tannery%soaking%"],
    "chrome_tanning": [
        "%tanning%chrome%", "%chrome%tanning%",
        "%tanning%leather%"],
    "retanning_dyeing": [
        "%retanning%", "%leather%dyeing%"],
    "leather_finishing": [
        "%finishing%leather%", "%leather%finishing%"],
    "foam_moulding": [
        "%polyurethane production%flexible foam%",
        "%foam%moulding%", "%foam%molding%"],
    "foam_cutting": [
        "%foam%cutting%", "%cutting%foam%"],
    "foam_lamination": [
        "%foam%lamination%", "%laminating%foam%"],
    "injection_moulding": [
        "%injection moulding%", "%injection molding%"],
    "thermoforming": [
        "%thermoforming%"],
    "polymer_extrusion": [
        "%extrusion%plastic%", "%polymer%extrusion%",
        "%plastic%extrusion%"],
    "vulcanisation": [
        "%vulcanisation%", "%vulcanization%"],
    "rubber_moulding": [
        "%rubber%moulding%", "%rubber%molding%"],
    "calendering_rubber": [
        "%calendering%rubber%", "%rubber%calendering%"],
    "washing_sanitising": [
        "%washing%feather%", "%washing%down%"],
    "sorting_grading": [
        "%sorting%feather%", "%sorting%down%"],
    "drying": [
        "%drying%feather%", "%drying%textile%"],
    "cork_boiling": [
        "%boiling%cork%", "%cork%boiling%",
        "%cork%processing%"],
    "cork_pressing": [
        "%pressing%cork%", "%cork%pressing%"],
    "cork_grinding": [
        "%grinding%cork%", "%cork%grinding%"],
}

PACKAGING_PATTERNS = {
    "Paper/Cardboard": [
        "%containerboard production%linerboard%containerboard%",
        "%corrugated board box production%corrugated board box%",
        "%containerboard production%",
        "%corrugated board%production%",
        "%kraft paper%production%"],
    "Plastic": [
        "%packaging film%production%low density polyethylene%",
        "%packaging film%production%",
        "%polyethylene production, low density, granulate"
        "%polyethylene, low density, granulate%",
        "%polyethylene%film%production%",
        "%polypropylene%film%production%"],
    "Glass": [
        "%packaging glass production%packaging glass%",
        "%packaging glass%production%"],
    "Other/Unspecified": [
        "%corrugated board box production%corrugated board box%",
        "%containerboard production%"],
}
