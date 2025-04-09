Roadmap
========

.. mermaid::

    gantt
        title PL2025 Timeline
        dateFormat  YYYY-MM-DD
        excludes weekends
        
        section Plan / Communication
        Stakeholder planning & prioritization    :done, des1, 2024-10-01, 2025-01-31
        Team F2F meeting, Bologna               :done, 2024-10-28, 5d
        Inform ALMA AQUA and the archive about new products :active, milestone, 2025-05-01, 1d

        section Development
        Infrastructure / Core library / Documentation  :active, 2024-10-01, 2025-05-01
        Implementation of New features / Heuristics :active, 2025-01-01, 2025-07-15
        Major new development well underway     :active, milestone, 2025-06-15, 1d
        Infrastructure work begins               :active, milestone, 2025-10-01, 1d

        section Testing (tentative schedule)
        Validation testing / bug fixing             :test1, 2025-06-15, 2025-09-07
        EE12 ALMA Test Report Review #1 (TRR1)      :test2, 2025-05-15, 2025-05-29
        EE12 ALMA Test Report Review #2 (TRR2)      :test3, 2025-07-10, 2025-07-24
        EE12 ALMA Acceptance Review                 :test4, 2025-08-19, 2025-09-04
        JAO-ARC Validation Subset testing           :test5, 2025-09-07, 2025-09-14

        section Delivery
        ALMA Cycle 11 and VLA/SRDP Pipeline (2024.1.0.8, CASA 6.6.1-py3.8) :done, crit, milestone, 2024-09-15, 1d
        VLA/SRDP Piepline (2024.1.1, CASA 6.6.6-py3.10) :done, crit, milestone, 2025-03-17, 1d
        ALMA End to End Cycle 12 (E2E12) *pre-release* :active, crit, milestone, 2025-08-31, 1d
        ALMA Cycle 12 and VLA/SRDP Pipeline (2025.*, CASA 6.7.1-py3.10) :active, crit, milestone, 2025-09-15, 1d
        VLASS Pipeline (2025.*, CASA 6.7.1-py3.10-hpg-enabled) :active, crit, milestone, 2025-09-15, 1d

.. mermaid::

    ---
    title: PL2024/2025 Branching
    ---
    gitGraph
    commit id: "feature for PL2024"
    commit id: "bugfix for PL2024"
    branch "release/2024.1.0 (casa-6.6.1)"
    checkout "release/2024.1.0 (casa-6.6.1)"
    commit id: "bugfix for ALMA/VLA-PL2024"
    checkout main
    commit id: "bugfix for VLA-PL2025"
    branch "release/2024.1.1 (casa-6.6.1)"
    checkout "release/2024.1.1 (casa-6.6.1)"
    commit id: "features for SRDP-PL2024"
    commit id: "bugfix for SRDP-PL2024"         
    checkout main
    merge "release/2024.1.1 (casa-6.6.1)"
    commit id: "refactoring"
    commit id: "major doc. updates"
    commit id: "testing updates"
    branch "feature"
    checkout "feature"
    commit id: "feature dev-1"
    commit id: "feature dev-2"
    checkout main
    merge "feature"
    branch "bugfix"
    checkout "bugfix"
    commit id: "bugfix update"
    checkout main
    merge "bugfix"
    commit id: "feature for ALMA"   
    merge "feature"   
    commit id: "feature for VLA"
    commit id: "feature for VLASS"   
    commit id: "minior doc updates"
    branch "release/alma|vla|srdp-pl2025 (casa-6.6.6)"
    checkout "release/alma|vla|srdp-pl2025 (casa-6.6.6)"
    commit id: "bugfix for ALMA"
    commit id: "bugfix for VLA"
    commit id: "bugfix for SRDP"
    checkout main
    commit id: "bugfix for VLASS"
    branch "release/vlass-pl2025 (casa-6.7.1-hpg)"
    checkout "release/vlass-pl2025 (casa-6.7.1-hpg)"
    commit id: "hotfix for VLASS"
    checkout main
    commit id: "bugfix for PL2026"
    commit id: "feature for PL2026"    