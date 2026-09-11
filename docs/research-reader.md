# Research Packet Navigation

The website's packet links use `/research/<slug>?view=reader`. This reading view
adds shared navigation assets to a verified copy of the report. It does not edit
the finalized HTML files, registry, charts, research, disclosures or source links.

At 960px and wider all sections wrap into visible rows. Narrow desktop windows,
tablets and phones use a sticky, keyboard-accessible "Jump to section" menu with
normal vertical scrolling. Choosing a section closes the menu and positions its
heading below navigation. The current section is marked while reading. Without
JavaScript, the links wrap in normal document flow. Print hides navigation.

The canonical `/research/<slug>` URL intentionally remains byte-exact, including
its ETag and no-transform caching behavior. Producer publication checks and
existing external canonical links continue to receive the original document.
The reading view points back to that canonical URL and has its own content ETag.
Asset hashes invalidate its ETag when the navigation code changes. No importer,
producer or publication-verification changes are required. Readers who enter a
canonical link directly still see the original navigation; website research and
comparison links open the enhanced view. Research history also offers a download
of each original document at its canonical URL, retaining the exact link required
by the existing publication verifier.

Verification: all 165 Python tests passed. `node scripts/qa_research_reader.cjs`
checked 745 section jumps across all ten packets at 320, 390, 521, 785 and 1280px,
including keyboard menu access, Escape, focus placement, reload/deep links, Back
navigation and no-JavaScript fallback. Desktop and narrow-window screenshots were
visually reviewed. No new page or navigation overflow was introduced. SAIL's
original KPI layout has a pre-existing 5px overflow at 320px; this navigation-only
change leaves the report content alone. No finalized packet or catalog changed.

This is part of the unpublished brand preview, pending Colton's review.
