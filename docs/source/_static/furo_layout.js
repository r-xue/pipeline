/**
 * furo_layout.js
 *
 * Adds a collapse/expand toggle button to the right-side TOC drawer in the
 * Furo Sphinx theme. The collapsed state is persisted in localStorage so it
 * survives page navigation.
 *
 * Only activates on wide screens (>82em) where Furo shows the TOC inline.
 * On smaller screens Furo's own mobile TOC overlay takes over.
 */
(function () {
    'use strict';

    function init() {
        var tocDrawer = document.querySelector('.toc-drawer');
        if (!tocDrawer) return;

        // Only install on wide screens where the inline TOC is visible.
        if (!window.matchMedia('(min-width: 83em)').matches) return;

        var btn = document.createElement('button');
        btn.className = 'toc-toggle-btn';
        btn.setAttribute('aria-label', 'Toggle table of contents');
        btn.setAttribute('title', 'Toggle table of contents');
        // chevron pointing right = TOC is visible, click to collapse
        btn.textContent = '\u276F';
        tocDrawer.appendChild(btn);

        // Restore saved state.
        if (localStorage.getItem('furo-toc-collapsed') === 'true') {
            tocDrawer.classList.add('toc-collapsed');
            var mainEl = tocDrawer.parentElement;
            if (mainEl) mainEl.classList.add('toc-collapsed');
            btn.textContent = '\u276E';  // chevron left = click to expand
        }

        btn.addEventListener('click', function () {
            var collapsed = tocDrawer.classList.toggle('toc-collapsed');
            var mainEl = tocDrawer.parentElement;
            if (mainEl) mainEl.classList.toggle('toc-collapsed', collapsed);
            localStorage.setItem('furo-toc-collapsed', collapsed);
            btn.textContent = collapsed ? '\u276E' : '\u276F';
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
}());
